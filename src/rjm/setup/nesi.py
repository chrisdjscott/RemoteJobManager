
import os
import uuid
import logging
import tempfile

import globus_sdk
from globus_sdk import GCSClient, TransferClient, DeleteData
from globus_sdk.services.gcs.data import GuestCollectionDocument
from globus_sdk.scopes import TransferScopes

from rjm import utils


logger = logging.getLogger(__name__)

GLOBUS_NESI_COLLECTION = '763d50ee-e814-4080-878b-6a8be5cf7570'
GLOBUS_NESI_ENDPOINT = 'd8223624-a701-41b0-859b-e88d4dd4b4d8'
GLOBUS_NESI_GCS_ADDRESS = "b09844.75bc.data.globus.org"
GLOBUS_COMPUTE_NESI_ENDPOINT = "63c0b682-43d1-4b97-bf23-6a676dfdd8bd"


class NeSISetup:
    """
    Runs setup steps specific to NeSI:

    - open SSH connection to Mahuika login node
    - configure globus compute endpoint
    - start globus compute endpoint
    - install scrontab entry to persist default endpoint (TODO: also, restart if newer version of endpoint?)

    """
    def __init__(self, username, account):
        self._username = username
        self._account = account

        # initialise values we are setting up
        self._globus_id = None  # globus endpoint id
        self._globus_path = None  # path to globus share

    def get_globus_compute_config(self):
        """Return globus compute config values"""
        return GLOBUS_COMPUTE_NESI_ENDPOINT

    def get_globus_transfer_config(self):
        return self._globus_id, self._globus_path

    def _handle_globus_auth(self, token_file, request_scopes, authoriser_scopes, by_scopes=True):
        """Login to globus and return authoriser"""
        print("="*120)
        print("Authorising Globus - this should open a browser where you need to authenticate with Globus and approve access")
        print("                     Globus is used by RJM to transfer files to and from NeSI")
        print("")
        print("NOTE: If you are asked for a linked identity with NeSI Keycloak please do one of the following:")
        print(f"      - If you already have a linked identity it should appear in the list like: '{self._username}@iam.nesi.org.nz'")
        print("        If so, please select it and follow the instructions to authenticate with your NeSI credentials")
        print("      - Otherwise, choose the option to 'Link an identity from NeSI Keycloak'")
        print("")
        print("="*120)

        # globus auth
        globus_cli = utils.handle_globus_auth(
            request_scopes,
            token_file=token_file,
        )

        if by_scopes:
            authorisers = globus_cli.get_authorizers_by_scope(requested_scopes=authoriser_scopes)
        else:
            authorisers = globus_cli.get_authorizers(requested_scopes=authoriser_scopes)

        return authorisers

    def _confirm_remote_write_permissions(self, transfer_client: TransferClient, path: str):
        """Confirms write access to the remote directory."""
        # create a temporary directory in the remote directory and then delete it
        tmp_path = f"{path}/testwriteperms.{str(uuid.uuid4())}"
        try:
            # create the directory
            logger.debug(f"Creating remote dir to test write permissions: {tmp_path}")
            transfer_client.operation_mkdir(GLOBUS_NESI_COLLECTION, path=tmp_path)

            # delete the tmp directory
            delete_data = DeleteData(transfer_client, GLOBUS_NESI_COLLECTION, recursive=True, notify_on_succeeded=False)
            delete_data.add_item(tmp_path)
            transfer_client.submit_delete(delete_data)

        except Exception as e:
            logger.error(f"Failed to confirm write permissions on remote directory: {e}")

        logger.debug("Remote write permissions confirmed")

    def setup_globus_transfer(self):
        """
        Sets up globus transfer:

        1. Create a directory for the guest collection
        2. Create a guest collection to share the directory
        3. Report back the endpoint id and url for managing the new guest collection

        """
        print("Setting up Globus Transfer, please wait...")

        # select directory for sharing
        guest_collection_dir = f"/nesi/nobackup/{self._account}/{self._username}/rjm-jobs"
        print("="*120)
        print(f"Creating Globus guest collection at:\n    {guest_collection_dir}")
        response = input("Press enter to accept the above location or specify an alternative here: ").strip()
        if len(response):
            if response.startswith("/nesi/nobackup"):
                guest_collection_dir = response
            else:
                raise ValueError("Valid guest collection directories must start with '/nesi/nobackup'")
        logger.info(f"Guest collection directory: {guest_collection_dir}")
        print("Continuing, please wait...")

        # request globus scopes for creating collection and transfer client
        endpoint_scope = GCSClient.get_gcs_endpoint_scopes(GLOBUS_NESI_ENDPOINT).manage_collections
        collection_scope = GCSClient.get_gcs_collection_scopes(GLOBUS_NESI_COLLECTION).data_access
        create_collection_scope = f"{endpoint_scope}[*{collection_scope}]"
        logger.debug(f"Create collection scope: {create_collection_scope}")
        logger.debug(f"Transfer scope: {TransferScopes.all}")
        required_scopes = [create_collection_scope, TransferScopes.all]
        authoriser_scopes = [endpoint_scope, TransferScopes.all]
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_token_file = os.path.join(tmpdir, "tokens.json")

            # requesting the scopes for creating the guest collection and also for transfer
            authorisers = self._handle_globus_auth(tmp_token_file, required_scopes, authoriser_scopes)

            # store for use later
            gcs_authoriser = authorisers[endpoint_scope]

            # setting up the transfer client with consents if required
            try:
                transfer_client = TransferClient(authorizer=authorisers[TransferScopes.all])
                logger.debug(f"Attempting to list NeSI filesystem: {guest_collection_dir}")
                response = transfer_client.operation_ls(GLOBUS_NESI_COLLECTION, path="/")

            except globus_sdk.TransferAPIError as err:
                if not err.info.consent_required:
                    raise

                # request the new scopes for the consent
                consent_required_scopes = err.info.consent_required.required_scopes
                logger.debug(f"Consent required scopes: {consent_required_scopes}")
                authorisers = self._handle_globus_auth(tmp_token_file, consent_required_scopes, [TransferScopes.all], by_scopes=False)
                transfer_client = TransferClient(authorizer=authorisers["transfer.api.globus.org"])
                logger.debug(f"Attempting to list NeSI filesystem after consents: {guest_collection_dir}")
                response = transfer_client.operation_ls(GLOBUS_NESI_COLLECTION, path="/")

            # we should now have a working transfer client

            # create the directory, if it succeeds all good, if it fails due to already existing we need to check access if ok eg by making a subdir (we could always do that to be safe)
            logger.debug(f"Attempting to create remote directory: {guest_collection_dir}")
            try:
                response = transfer_client.operation_mkdir(GLOBUS_NESI_COLLECTION, path=guest_collection_dir)
            except globus_sdk.TransferAPIError as err:
                if err.code == "ExternalError.MkdirFailed.Exists":
                    logger.debug("Directory already exists; confirming write access")
                    self._confirm_remote_write_permissions(transfer_client, guest_collection_dir)
                else:
                    raise

            # GCS client
            client = GCSClient(GLOBUS_NESI_GCS_ADDRESS, authorizer=gcs_authoriser)

            # user credentials
            cred = client.get("/user_credentials")
            assert cred["code"] == "success", "Error getting user_credentials"
            try:
                user_cred = cred["data"][0]
            except IndexError as exc:
                logger.error("Error retrieving user credentials")
                print("user_credentials:")
                print(cred)
                raise exc

            # collection document specifying options for new guest collection
            doc = GuestCollectionDocument(
                mapped_collection_id=GLOBUS_NESI_COLLECTION,
                user_credential_id=user_cred["id"],
                collection_base_path=guest_collection_dir,
                display_name="RJM_jobs",
                public=False,
                enable_https=True,
            )
            logger.debug(f"Collection document: {doc}")

            # create Globus collection, report back endpoint id for config
            response = client.create_collection(doc)
            endpoint_id = response.data["id"]
            logger.debug(f"Created Globus Guest Collection with Endpoint ID: {endpoint_id}")

        # report endpoint id for configuring rjm
        print("="*120)
        print(f"Globus guest collection endpoint id: '{endpoint_id}'")
        print(f"Globus guest collection endpoint path: '{guest_collection_dir}'")
        print("You can manage the endpoint you just created online at:")
        print(f"    https://app.globus.org/file-manager/collections/{endpoint_id}/overview")
        print("="*120)

        # also store the endpoint id and path
        self._globus_id = endpoint_id
        self._globus_path = guest_collection_dir

from unittest import mock

import pytest

from rjm.setup import nesi
from rjm.setup.nesi import NeSISetup


@pytest.fixture
def globus_mocks(mocker):
    real_gcs_client = nesi.GCSClient
    endpoint_scope = real_gcs_client.get_gcs_endpoint_scopes(nesi.GLOBUS_NESI_ENDPOINT).manage_collections
    mocked_auth = mocker.patch.object(
        NeSISetup,
        "_handle_globus_auth",
        return_value={endpoint_scope: mock.Mock(), nesi.TransferScopes.all: mock.Mock()},
    )
    mocked_transfer_client = mocker.patch("rjm.setup.nesi.TransferClient")
    mocked_gcs_client = mocker.patch("rjm.setup.nesi.GCSClient")
    mocked_gcs_client.get_gcs_endpoint_scopes = real_gcs_client.get_gcs_endpoint_scopes
    mocked_gcs_client.get_gcs_collection_scopes = real_gcs_client.get_gcs_collection_scopes
    gcs = mocked_gcs_client.return_value
    gcs.get.return_value = {"code": "success", "data": [{"id": "cred-id"}]}
    gcs.create_collection.side_effect = [
        mock.Mock(data={"id": "endpoint-1"}),
        mock.Mock(data={"id": "endpoint-2"}),
    ]

    return mocked_auth, mocked_transfer_client.return_value, gcs


def test_setup_globus_transfer_multiple_projects(mocker, globus_mocks):
    mocked_auth, transfer_client, gcs = globus_mocks
    mocker.patch("builtins.input", return_value="")

    setup = NeSISetup("user", ["uoa00106", "nesi12345"])
    setup.setup_globus_transfer()

    # a single Globus login covers every project
    mocked_auth.assert_called_once()
    assert gcs.create_collection.call_count == 2
    assert setup.get_globus_transfer_config() == {
        "uoa00106": ("endpoint-1", "/nesi/nobackup/uoa00106/user/rjm-jobs"),
        "nesi12345": ("endpoint-2", "/nesi/nobackup/nesi12345/user/rjm-jobs"),
    }
    mkdir_paths = [c.kwargs["path"] for c in transfer_client.operation_mkdir.call_args_list]
    assert mkdir_paths == ["/nesi/nobackup/uoa00106/user/rjm-jobs", "/nesi/nobackup/nesi12345/user/rjm-jobs"]
    display_names = [c.args[0]["display_name"] for c in gcs.create_collection.call_args_list]
    assert display_names == ["RJM_jobs_uoa00106", "RJM_jobs_nesi12345"]


def test_select_collection_dir_custom(mocker):
    mocker.patch("builtins.input", return_value="/nesi/nobackup/uoa00106/shared/rjm")

    setup = NeSISetup("user", ["uoa00106"])

    assert setup._select_collection_dir("uoa00106") == "/nesi/nobackup/uoa00106/shared/rjm"


def test_select_collection_dir_invalid(mocker):
    mocker.patch("builtins.input", return_value="/home/user/rjm")

    setup = NeSISetup("user", ["uoa00106"])

    with pytest.raises(ValueError, match="/nesi/nobackup"):
        setup._select_collection_dir("uoa00106")

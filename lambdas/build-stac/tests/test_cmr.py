from unittest.mock import patch
from utils.stac import generate_stac_cmrevent, from_cmr_links


def test_generate_stac_cmrevent(
    cmr_json_example, sample_assets, cmr_multi_asset_sample_event
):
    with patch("os.environ.get") as mock_os_environ_get, patch(
        "utils.role.assume_role"
    ) as mock_role_assume_role, patch("utils.stac.GranuleQuery.get") as mock_get, patch(
        "utils.stac.from_cmr_links"
    ) as mock_get_assets:
        mock_os_environ_get.return_value = "my_role_arn"
        mock_role_assume_role.return_value = {
            "AccessKeyId": "my_access_key_id",
            "SecretAccessKey": "my_secret_access_key",
            "SessionToken": "my_session_token",
        }
        mock_get.return_value = [cmr_json_example]
        mock_get_assets.return_value = ([], sample_assets)
        result = generate_stac_cmrevent(cmr_multi_asset_sample_event)
        assert len(result.assets) == 3
        assert (
            result.id
            == "uavsar_AfriSAR_v1-coreg_fine_lopenp_14043_16008_140_009_160225_kz"
        )


def test_from_cmr_links(cmr_json_example, cmr_multi_asset_sample_event):
    with patch("utils.stac.generate_asset") as mock_generate_asset:
        mock_generate_asset.return_value = "mocked_asset"
        with patch("utils.stac.generate_link") as mock_generate_link:
            mock_generate_link.return_value = "mocked_link"
            links, assets = from_cmr_links(
                cmr_json_example["links"], cmr_multi_asset_sample_event
            )
            assert len(links) == 1
            assert len(assets) == 3

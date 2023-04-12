from unittest.mock import patch

import pytest
from utils.stac import generate_stac_cmrevent


def test_generate_stac_cmrevent(
    cmr_json_example, sample_assets, cmr_multi_asset_sample_event
):
    with patch("utils.stac.GranuleQuery.get") as mock_get:
        mock_get.return_value = [cmr_json_example]
        with patch("utils.stac.get_assets_from_cmr") as mock_get_assets:
            mock_get_assets.return_value = sample_assets
            result = generate_stac_cmrevent(cmr_multi_asset_sample_event)

            assert len(result.assets) == 3
            assert (
                result.id
                == "uavsar_AfriSAR_v1-coreg_fine_lopenp_14043_16008_140_009_160225_kz"
            )

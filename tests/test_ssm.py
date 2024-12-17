"""Unit tests of the delta_utils.ssm module"""

from delta_utils import ssm


def test_ssm_params(mocker):
    """Test that parameters to SSM client are set correct."""
    mocker.patch.object(ssm.boto3, "client")

    prefix = "/dev/testapp/"
    ssm_response = {
        "Parameters": [
            {
                "Name": f"{prefix}paramName1",
                "Type": "String",
                "Value": "value1",
                "Version": 1,
            },
            {
                "Name": f"{prefix}paramName2",
                "Type": "String",
                "Value": "value2",
                "Version": 2,
            },
        ]
    }

    mocked_client = ssm.boto3.client.return_value
    mocked_client.get_parameters_by_path.return_value = ssm_response

    actual_response = ssm.ssm_params(prefix)

    ssm.boto3.client.assert_called_once_with("ssm", region_name="eu-west-1")
    expected_response = {
        "paramName1": ssm_response["Parameters"][0]["Value"],
        "paramName2": ssm_response["Parameters"][1]["Value"],
    }

    assert actual_response == expected_response

    mocked_client.get_parameters_by_path.assert_called_once_with(
        Path=prefix, Recursive=True, WithDecryption=True
    )


def test_ssm_params_no_trailing_slash(mocker):
    """Test that parameters to SSM client are set correct."""
    mocker.patch.object(ssm.boto3, "client")

    prefix = "/dev/testapp"
    ssm_response = {
        "Parameters": [
            {
                "Name": f"{prefix}/paramName1",
                "Type": "String",
                "Value": "value1",
                "Version": 1,
            }
        ]
    }

    mocked_client = ssm.boto3.client.return_value
    mocked_client.get_parameters_by_path.return_value = ssm_response

    actual_response = ssm.ssm_params(prefix)

    ssm.boto3.client.assert_called_once_with("ssm", region_name="eu-west-1")
    expected_response = {"paramName1": ssm_response["Parameters"][0]["Value"]}

    assert actual_response == expected_response

    mocked_client.get_parameters_by_path.assert_called_once_with(
        Path=f"{prefix}/", Recursive=True, WithDecryption=True
    )

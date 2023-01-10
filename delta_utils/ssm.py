import boto3

AWS_REGION = "eu-west-1"


def ssm_params(path: str) -> dict:
    """Returns a dictionary with all settings from the path"""
    ssm = boto3.client("ssm", region_name=AWS_REGION)
    results = {}
    if not path.endswith("/"):
        path += "/"
    kwargs = {"Path": path, "Recursive": True, "WithDecryption": True}

    while True:
        response = ssm.get_parameters_by_path(**kwargs)

        for parameter in response["Parameters"]:
            env_name = parameter["Name"][len(path) :]
            env_value = parameter["Value"]
            results[env_name] = env_value

        if response.get("NextToken"):
            kwargs["NextToken"] = response["NextToken"]
            continue
        break
    return results

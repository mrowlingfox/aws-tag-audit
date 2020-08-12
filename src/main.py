import boto3
import boto3.session
import functools
from os import path
from os import makedirs
import json
import yaml
from validate_email import validate_email


REGION_NAME="ap-southeast-2"

def build_filepath(region_name, profile_name, client_type, method, key):
        filename = f'{region_name}/{profile_name}/{client_type}/{method}'
        if key:
            key = key.replace("/", "+")
            filename = f'{filename}_Key={key}'
        filename = f'{filename}.json'
        base_dir = './data/'
        filepath = base_dir + filename 
        return filepath

def check_file_exists(filepath):
    return path.isfile(filepath)

def read_from_file(filepath):
    with open(filepath) as f:
        data = json.load(f)
    return data

def write_to_file(filepath, data):
    filedir = filepath.rsplit('/', 1)[0]
    if not path.isdir(filedir):
        makedirs(filedir)
    with open(filepath, 'w+') as f:
        json.dump(data, f, ensure_ascii=False)

def local_cache(func):
    @functools.wraps(func)
    def wrapper_decorator(self, method, *args, **kwargs):
        profile_name = self._session.profile_name
        region_name = self._session.region_name
        key = kwargs.get('Key')
        filepath = build_filepath(region_name, profile_name, self._client_type, method, key)
        if check_file_exists(filepath):
            value = read_from_file(filepath)
        else:
            value = func(self, method, *args, **kwargs)
            write_to_file(filepath, value)
        return value
    return wrapper_decorator

def sort(func):
    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        value = func(*args, **kwargs)
        sorted_values = sorted(value)
        return sorted_values
    return wrapper_decorator


class TaggingApi():
    def __init__(self, session):
        self._session = session
        self._client_type = 'resourcegroupstaggingapi'
        self._client = self._session.client(self._client_type)

    @local_cache
    def _paginator(self, method, list_key, **kwarg):
        results = []
        paginator = self._client.get_paginator(method)
        for page in paginator.paginate(**kwarg):
            items = page.get(list_key)
            results = [*results, *items]
        return results

    @sort
    def get_keys(self):
        return self._paginator('get_tag_keys', 'TagKeys')

    @sort
    def get_values(self, key):
        return self._paginator('get_tag_values', 'TagValues', Key=key)

    def get_resources(self):
        return self._paginator('get_resources', 'ResourceTagMappingList')

    def get_keys_with_values(self):
        results = {}
        tag_keys = self.get_keys()
        for key in tag_keys:
            tag_values = self.get_values(key)
            results[key] = tag_values
        return results

STANDARD_TAGS=['Name', 'Environment','Project','Department','Contact','Management','MonitoringFlag']
REQUIRED_TAGS=['Name', 'Environment','Project','Department','Contact']

def is_valid_email(email):
    return validate_email(email_address=email, check_regex=True, check_mx=False, use_blacklist=True, debug=False)

def get_invalid_standard_tag_values(standard_tags_values):
    invalid_values = {}
    contacts = standard_tags_values.get('Contact',[])
    if contacts is not None:
        invalid_contacts = [contact for contact in contacts if not is_valid_email(contact)]
        if len(invalid_contacts) > 0:
            invalid_values['Contact'] = invalid_contacts
    
    environments = standard_tags_values.get('Environments',[])
    if environments is not None:
        invalid_environment = [env for env in environments if env not in ['staging','production', 'dev']]
        if len(invalid_environment) > 0:
            invalid_values['Environments'] = invalid_environment


def get_missing_required_tags(resources):
    missing_tags = {}
    for resource in resources:
        tags = resource.get('Tags')
        arn = resource.get('ResourceARN')
        required_tags = list(filter(lambda d: d['Key'] in REQUIRED_TAGS, tags))
        required_tags_keys = [t['Key'] for t in required_tags]
        missing_required_tags = [d for d in REQUIRED_TAGS if d not in required_tags_keys]
        if len(missing_required_tags) > 0:
            missing_tags[arn] = missing_required_tags
    return missing_tags

def write_to_yaml(profile, **kwargs):
    output_dir = 'output'
    if not path.isdir(output_dir):
        makedirs(output_dir)
    for key, value in kwargs.items():
        with open(f'{output_dir}/{profile}_{key}.yaml', 'w') as file:
            yaml.dump(value, file)

def get_profiles():
    session = boto3.session.Session(region_name=REGION_NAME)
    exclude_list = [
        "okta", 
        "gitops", 
        "martian-ott-nonprod", 
        "martian-ott-prod", 
        "foxsports-shared-services", 
        "dev", 
        "foxsports-olympus-nonprod", 
        "foxsports-olympus-prod"
    ]
    profiles = [ p for p in session.available_profiles if p not in exclude_list]
    return profiles

def get_regions():
    ec2 = boto3.client('ec2')
    response = ec2.describe_regions()
    regions = response['Regions']
    return [ region.get("RegionName") for region in regions ]


def main():
    profiles = get_profiles()
    regions = get_regions()
    for profile in profiles:
        for region in regions:
            try:
                print("=== ", profile, " === ", region, " ===")
                session = boto3.session.Session(profile_name=profile, region_name=region)
                api = TaggingApi(session)
                keys_with_values = api.get_keys_with_values()
                resources = api.get_resources()
                missing_required_tags = get_missing_required_tags(resources)
                write_to_yaml(f'{profile}_{region}',
                    missing_required_tags=missing_required_tags,
                )
                standard_tags_values = { tag: keys_with_values.get(tag) for tag in STANDARD_TAGS }
                if standard_tags_values:
                    invalid_standard_tag_values = get_invalid_standard_tag_values(standard_tags_values)
                    write_to_yaml(f'{profile}_{region}',
                        standard_tags_values=standard_tags_values,
                        invalid_standard_tag_values=invalid_standard_tag_values,
                    )
            except Exception as identifier:
                error_code = identifier.response.get("Error", {}).get("Code")
                if error_code != "AccessDeniedException":
                    print("Exception Profile: ", profile, region, identifier)


if __name__ == "__main__":
    main()

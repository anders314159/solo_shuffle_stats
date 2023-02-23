import json
from pprint import pprint

import click
import requests
import constants as C
import pandas as pd
import dask.dataframe as dd
from datetime import date
# TODO: For a larger project, adding a type for URLs might make sense


def generate_auth_header(credential_file: str, token_url: str = "https://oauth.battle.net/token") -> dict:
    """
    Returns an auth header dict with our bearer token
    :param credential_file: Flat JSON file with "id" and "secret"
    :param token_url: The URL to send the request to
    :return:
    """
    with open(credential_file) as f:
        credentials = json.load(f)
        basic_auth = requests.auth.HTTPBasicAuth(credentials['id'], credentials['secret'])
    token_response = requests.post(url=token_url, auth=basic_auth, data={"grant_type": "client_credentials"})
    bearer_token = token_response.json()['access_token']
    return {"Authorization": f"Bearer {bearer_token}"}


def get_leaderboard_names(auth_header: dict, season: int) -> list:
    """
    Returns the names of the different leaderboards, i.e. "2v2", "rogue-outlaw", "mage-frost", and so on.
    :param auth_header: Auth header for requests
    :param season: The WoW PVP season
    :return: A sorted list of leaderboards
    """
    # Get the different leaderboards,
    leaderboard_url = C.PVP_BASE_URL\
        .replace(C.REGION_TOKEN, 'us')\
        .replace(C.TYPE_TOKEN, 'index')\
        .replace(C.SEASON_TOKEN, str(season))
    leaderboards = requests.get(leaderboard_url, headers=auth_header).json()

    leaderboards = [leaderboard['name'] for leaderboard in leaderboards['leaderboards']]
    leaderboards.sort()
    return leaderboards


def get_all_request_urls(leaderboards: list, regions: list, season: int) -> pd.DataFrame:
    # Make URLs for each leaderboard in a dataframe
    df = pd.DataFrame()
    df.index = leaderboards
    for region in regions:
        region_url = C.PVP_BASE_URL.replace(C.REGION_TOKEN, region).replace(C.SEASON_TOKEN, str(season))
        leaderboards_and_region_urls = [region_url.replace(C.TYPE_TOKEN, leaderboard) for leaderboard in leaderboards]
        df[region] = leaderboards_and_region_urls
    return df


def get_and_store_results(df: pd.DataFrame, output_file: str, auth_header: dict):
    # Get data from each leaderboard
    meta = dd.utils.make_meta(df)
    ddf = dd.from_pandas(df, npartitions=8)

    # Kinda jank to have a function here, but dask require using map_partitions to hit all partitions, and it would be
    # a very long lambda without a function
    def applymap_requests(dataframe: pd.DataFrame):
        return dataframe.applymap(lambda url: requests.get(url, headers=auth_header).json())

    # Save data
    ddf.map_partitions(applymap_requests, meta=meta).compute().to_csv(output_file)

@click.command()
@click.option('--regions', default=['eu', 'us'], help='The regions to get data from', multiple=True)
@click.option('--season', default=34, type=int)
@click.option('--credential_file', default='credentials.json', type=str)
def get_and_store_ratings(regions, season, credential_file):
    auth_header = generate_auth_header(credential_file)
    leaderboards = get_leaderboard_names(auth_header, season)
    df = get_all_request_urls(leaderboards, regions, season)
    output_file = f'data/dask_{date.today()}.csv'
    get_and_store_results(df, output_file, auth_header)


if __name__ == "__main__":
    get_and_store_ratings()

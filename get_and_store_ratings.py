import json
import click
import requests
import pandas as pd
import dask.dataframe as dd
from datetime import date


@click.command()
@click.option('--regions', default=['eu', 'us'], help='The regions to get data from', multiple=True)
@click.option('--season', default=34, type=int)
@click.option('--credential_file', default='credentials.json', type=str)
def get_and_store_ratings(regions, season, credential_file):
    # Generate bearer token which we use for all subsequent requests
    token_url = "https://oauth.battle.net/token"
    with open(credential_file) as f:
        credentials = json.load(f)
        basic_auth = requests.auth.HTTPBasicAuth(credentials['id'], credentials['secret'])
    token_response = requests.post(url=token_url, auth=basic_auth, data={"grant_type": "client_credentials"})
    bearer_token = token_response.json()['access_token']
    auth_header = {"Authorization": f"Bearer {bearer_token}"}

    # Make template API URLs - see https://develop.battle.net/documentation/world-of-warcraft/game-data-apis
    region_token = '%REGION%'
    type_token = '%TYPE%'
    pvp_base_url = f'https://{region_token}.api.blizzard.com/data/wow/pvp-season/{season}/pvp-leaderboard/{type_token}?namespace=dynamic-{region_token}'

    # Get the different leaderboards, i.e. 2v2, rogue-outlaw, mage-frost, and so on.
    leaderboard_url = pvp_base_url.replace(region_token, 'us').replace(type_token, 'index')
    leaderboards = requests.get(leaderboard_url, headers=auth_header).json()
    leaderboards = [leaderboard['name'] for leaderboard in leaderboards['leaderboards']]
    leaderboards.sort()

    # Make URLs for each leaderboard in a dataframe
    df = pd.DataFrame()
    df.index = leaderboards
    for region in regions:
        region_url = pvp_base_url.replace(region_token, region)
        leaderboards_and_region_urls = [region_url.replace(type_token, leaderboard) for leaderboard in leaderboards]
        df[region] = leaderboards_and_region_urls

    # Get data from each leaderboard
    meta = dd.utils.make_meta(df)
    ddf = dd.from_pandas(df, npartitions=8)

    # Kinda jank to have a function here, but dask require using map_partitions to hit all partitions, and it would be
    # a very long lambda without a function
    def applymap_requests(dataframe: pd.DataFrame):
        return dataframe.applymap(lambda url: requests.get(url, headers=auth_header).json())

    # Save data
    ddf.map_partitions(applymap_requests, meta=meta).compute().to_csv(f'data/dask_{date.today()}.csv')


if __name__ == "__main__":
    get_and_store_ratings()

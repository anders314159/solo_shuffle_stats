import dask
import json
import click
import requests

@click.command()
@click.option('--regions', default=['eu', 'us'], help='The regions to get data from', multiple=True)
@click.option('--season', default=34, type=int)
def get_and_store_ratings(regions, season):
    print(regions)
    print(season)

    # Generate token
    with open('credentials.json') as f:







if __name__ == "__main__":
    get_and_store_ratings()
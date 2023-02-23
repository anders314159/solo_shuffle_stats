# Make template API URLs - see https://develop.battle.net/documentation/world-of-warcraft/game-data-apis
REGION_TOKEN = '%REGION%'
SEASON_TOKEN = '%SEASON%'
TYPE_TOKEN   = '%TYPE%'
PVP_BASE_URL = f'https://{REGION_TOKEN}.api.blizzard.com/data/wow/pvp-season/{SEASON_TOKEN}/pvp-leaderboard/{TYPE_TOKEN}?namespace=dynamic-{REGION_TOKEN}'

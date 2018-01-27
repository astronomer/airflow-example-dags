from airflow.hooks.base_hook import BaseHook
from nba_py import player, game, team, constants


class NbaHook(BaseHook):

    def __init__(self, endpoint, id, method, stats):

        self.endpoint = endpoint
        self.method = method
        self.id = id
        self.stats = stats

    def call(self):

        endpoint = self.endpoint
        method = self.method
        stats = self.stats
        id = self.id

        methodmap = {
            'player': player,
            'game': game,
            'team': team,
            'constants': constants
            }
        # test_get_attribute = getattr(methodmap[endpoint], method)
        # print("THIS IS IT\n")
        # print(test_get_attribute)
        # print("THIS IS IT\n")
        return getattr(getattr(methodmap[endpoint], method)(id, season='2017-18'), stats)()

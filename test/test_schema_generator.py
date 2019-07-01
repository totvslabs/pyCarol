import unittest
import sys
import json
sys.path.append('../.')
from pycarol.schema_generator import carolSchemaGenerator

class SchemaTestCase(unittest.TestCase):
    
    def test_assert(self):

        tweet = {"created_at": "2019-04-06T04:20:41", "id": 1114382394935652352, "id_str": "1114382394935652352",
                 "text": "@YazCrypto If the price goes down.  That will be bearish.  $LINK",
                 "source": "<a href=\"http://twitter.com/download/android\" rel=\"nofollow\">Twitter for Android</a>",
                 "in_reply_to_screen_name": "YazCrypto",
                 "user": {"id": 930238793512292352, "id_str": "930238793512292352", "name": "Alexander",
                          "screen_name": "AlexSau37473923", "location": "Florida, USA", "url": None,
                          "description": "Started with $BTC mining back in 2013.  \n\n\nDiversified into:\n\n$LINK $XMR $DRGN $MAN $STK\n\nLong On Crypto.",
                          "protected": False, "verified": False, "followers_count": 87, "friends_count": 184,
                          "listed_count": 0, "favourites_count": 757, "statuses_count": 910,
                          "created_at": "2017-11-14T00:59:46", "utc_offset": None, "time_zone": None,
                          "geo_enabled": False, "lang": "en", "default_profile": True, "default_profile_image": False,
                          "notifications": None}, "geo": None, "coordinates": None, "place": None, "contributors": None,
                 "is_quote_status": False, "quote_count": 0, "reply_count": 0, "retweet_count": 0, "favorite_count": 0,
                 "entities": {"hashtags": [], "user_mentions": [
                     {"screen_name": "YazCrypto", "name": "Yaz", "id": 965803417175142400,
                      "id_str": "965803417175142400", "indices": [0, 10]}],
                              "symbols": [{"text": "LINK", "indices": [59, 64]}]}, "filter_level": "low", "lang": "en",
                 "tweet_url": "https://twitter.com/AlexSau37473923/status/1114382394935652352",
                 "coins_mentioned": ["LINK"], "coins_mentioned_count": 1, "sentiment_score": 0.5944444444444444,
                 "sentiment_score_subjectivity": 0.2888888888888889, "reaction_score": 1.295,
                 "crawler": "lightning-work-01"}

        self.assertEqual(1114382394935652352, tweet['id'])
        self.assertEqual('AlexSau37473923', tweet['user']['screen_name'])

        schema = carolSchemaGenerator(tweet)
        schema = schema.to_dict(mdmStagingType='tweet', mdmFlexible=False, export_data=False,
                                crosswalkname='tweet', crosswalkList=['id'])

        print(json.dumps(schema, indent=4))
        self.assertEqual('tweet', schema['mdmStagingType'])
        self.assertTrue('id' in schema['mdmStagingMapping']['properties'])
        self.assertTrue('id' in schema['mdmStagingMapping']['properties']['user']['properties'])


if __name__ == '__main__':
    unittest.main()
    
    
    

    
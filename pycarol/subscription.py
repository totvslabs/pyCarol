from .data_models.data_models import DataModel

class Subscription:
    """
    Subscription from carol.

    Args:

        Carol object
            Carol object.
    """

    def __init__(self, carol):
        self.carol = carol

    def get_all(self, offset=0, page_size=1000, sort_by=None, sort_order='ASC'):
        """
        Get all subscription information.

        Args:
            offset: `int`, default 0
            Offset for pagination. Only used when `scrollable=False`
        page_size: `int`, default 100
            Number of records downloaded in each pagination. The maximum value is 1000
        sort_order: `str`, default 'ASC'
            Sort ascending ('ASC') vs. descending ('DESC').
        sort_by: `str`,  default `None`
            Name to sort by.

        Returns: list
             list of subscription

        """
        query_params = {"offset": offset, "pageSize": page_size, "sortOrder": sort_order,
                        "sortBy": sort_by}
        return self.carol.call_api('v2/subscription', method='GET' ,params=query_params)['hits']

    def create_edit(self, subscription_list):
        """
        Subscribe to a entity or edit the subscription. Attention: Today the "pk"" for a subscription is
        `connector_id` + `dm_id`. It means that is possible to have two subscriptions for the same datamodel
        using different connectors.

        Args:
            subscription_list:
                list of subscriptions to create/edit.

        Returns: dict
            carol response.

        """


        return self.carol.call_api('v2/subscription/batch', method='POST', data=subscription_list)

    def pause(self, subscription_id):
        """
        Pause a subscription

        Args:
            subscription_id:
                if og the subscription

        Returns: dict
            carol response.

        """
        return self.carol.call_api(f'v2/subscription/{subscription_id}/pause', method='POST')


    def clear(self, subscription_id):
        """
        Clear the pending records from a subscription

        Args:
            subscription_id:
                if og the subscription

        Returns: dict
            carol response.

        """
        return self.carol.call_api(f'v2/subscription/{subscription_id}/clear', method='POST')

    def play(self, subscription_id):
        """
        Play a subscription

        Args:
            subscription_id:
                if og the subscription

        Returns: dict
            carol response.

        """
        return self.carol.call_api(f'v2/subscription/{subscription_id}/play', method='POST')


    def get_by_id(self, subscription_id):
        """
        Get a subscription by ID

        Args:
            subscription_id:
                if og the subscription

        Returns: dict
            carol response.

        """
        return self.carol.call_api(f'v2/subscription/{subscription_id}', method='GET')

    def get_info(self, subscription_id):
        """
        Get information for a subscription by ID

        Args:
            subscription_id:
                if og the subscription

        Returns: dict
            carol response.

        """
        return self.carol.call_api(f'v2/subscription/{subscription_id}/details', method='GET')

    def get_dm_subscription(self, dm_name=None, dm_id=None):
        """
        Get DataModel subscription information.

        Args:
            dm_name: `str` default `None`
                DataModel name
            dm_id: `str` default `None`
                DataModel id

        Returns: dict
            carol response.

        """

        if dm_name is not None:
            resp = DataModel(self.carol).get_by_name(dm_name)
            dm_id = resp['mdmId']
        elif dm_id is None:
            raise ValueError('Either `dm_name` or `dm_id` must be set.')

        DataModel(self.carol)
        return self.carol.call_api(f'v2/subscription/template/{dm_id}', method='GET')
















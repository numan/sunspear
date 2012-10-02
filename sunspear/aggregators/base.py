class BaseAggregator(object):
    def __init__(self, *args, **kwargs):
        pass

    def process(self, current_activities, original_activities, aggregators, *args, **kwargs):
        """
        Processes the activities performing any mutations necessary.
        :type current_activities: list
        :param current_activities: A list of activities, as they stand at the current stage of the aggregation pipeline
        :type original_activities: list
        :param original_activities: A list of activities before any processing by the aggregation pipeline
        :type aggregators: list
        :param aggregators: A list of aggregators in the current pipeline. The aggregators will be executed (or have been executed) in the order they appear in the list
        :return: A list of of activities
        """
        return current_activities




def get_metadata(search_string=None):
    elastic = Elasticsearch(elastic_host)

    # Get all ids
    elastic_search = Search(using=elastic, index=metadata_index)

    elastic_search = elastic_search.source([])  # only get ids
    ids = [h.meta.id for h in elastic_search.scan()]
    if ids:
        return {
            'status': True,
            'message': 'List of platform codes',
            'result': ids
        }, 201
    else:
        return {
            'status': True,
            'message': 'List of platform codes - empty',
            'result': ids
        }, 201

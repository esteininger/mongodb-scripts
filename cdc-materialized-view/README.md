# Change Data Capture into Materialized View

## Explanation

1. Create an aggregation pipeline filter, so that any event that is an insert, into checking account, and negative then the [watch method (CDC)](https://docs.mongodb.com/manual/changeStreams/), picks it up
2. Events that pass this filter, are then passed as a document
3. We're grabbing the [document ID](https://docs.mongodb.com/manual/reference/method/ObjectId/)
4. We're then merging these updates into a new collection via [$merge](https://docs.mongodb.com/manual/reference/operator/aggregation/merge/) so that whenever one of these documents matches an existing document on the "email" string, it will replace. If there is no email match, it will insert.

### Code

``` python
# Change stream pipeline
pipeline = [
    {'$match': {'operationType': 'insert'}},
    {'$match': {'fullDocument.accounts.type': 'checking'}},
    {'$match': {'fullDocument.accounts.balance': {"$lt": 0}}}
]

# start change stream listener
for document in accounts_collection.watch(pipeline=pipeline, full_document='updateLookup'):
    result = "=== INSERT EVENT ===\n"

    # incremental materialized view
    document_id = document['fullDocument']['_id']

    # for each document that matches filter above
    # merge into "accounts_materialized_view" collection
    materialized_agg = [
        {
            '$match': {
                '_id': ObjectId(document_id)
            }
        },
        {
            '$project': {
                '_id':1,
                'email':1
            }
        },
        {
            '$merge': {
                'into': 'accounts_materialized_view',
                'on': 'email',
                'whenMatched': 'replace',
                'whenNotMatched': 'insert'
            }
        }
    ]
    accounts_collection.aggregate(materialized_agg)

```

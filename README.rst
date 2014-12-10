BynamoDB
========

High level DynamoDB API wrapping boto.dynamodb2.layer1.DynamoDBConnection

Patch DynamoDBConnection
========================

Set DynamoDBConnection default host and port.

.. code-block:: python

    from bynamodb import patch_dynamodb_connection

    patch_dynamodb_connection(host='localhost', port=8000)

Model Definition
================
.. code-block:: python

    from bynamodb.attributes import StringAttribute
    from bynamodb.indexes import GlobalAllIndex
    from bynamodb.model import Model
    
    class Article(Model):
        published_at = StringAttribute(hash_key=True)
        id = StringAttribute(range_key=True)
        title = StringAttribute()
        content = StringAttribute()
        author = StringAttribute()
        
        class AuthorIndex(GlobalAllIndex):
            read_throughput = 5
            write_throughput = 5
            hash_key = 'author'
            range_key = 'published_at'

Put Item & Get Item
===================
.. code-block:: python

    Article.put_item({
        'published_at': '2014-12-09',
        'id': '1',
        'title': 'This is the title',
        'content': 'This is the content',
        'author': 'Bochul Choi'
    })
    article = Article.get_item(hash_key='2014-12-09', range_key='1')

Simple Scan & Query
===================
.. code-block:: python

    # Scan all articles that the title starts with "Title"
    articles = Article.scan(title__startswith='Title')

    # Query articles that author is "Bochul Choi"
    articles = Article.query(author__eq='Bochul Choi', index_name='AuthorIndex')

Complex lookups in Scan & Query
===============================
.. code-block:: python

    from bynamodb.filterexps import Contains, GT
    
    keyword = 'bynamodb'
    filter_exp = GT('published_at', '2014-12-01').apply_and(
        Contains('title', keyword).apply_or(Contains('content', keyword))
    ) # published_at > '2014-12-01' and (keyword in title or keyword in content)
    
    # Scan all articles that match the filter expression
    articles = Article.scan(filter_exp)
    
    # Query articles that match the filter expression and the author condition
    author = 'Bochul Choi'
    articles = Atricle.query(author__eq: author, filter_builder=filter_exp,
                             index_name='AuthorIndex')
    
    

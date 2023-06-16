from delta_utils.pii import Consumer, Producer


def test_pii(spark, setup_pii_table):
    setup_pii_table()
    producer = Producer(spark)
    producer.create_removal_request(
        affected_table="beta_live.db1.table",
        source_table="alpha_live.db1.table",
        source_identifying_attributes=[("id", "1")],
    )

    consumer = Consumer(spark, consumer="beta_live")
    assert consumer.get_removal_requests().count() == 1
    row = consumer.get_removal_requests().first()
    consumer.mark_as_completed(row.id)
    assert consumer.get_removal_requests().count() == 0

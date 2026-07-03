"""
test_sns.py — testes E2E para operações SNS via shim.

Cobre as operações básicas de tópicos, subscriptions e publish,
incluindo fan-out e filter policy.

Assume:
    - shim rodando em http://localhost:4566 (AUTH_MODE=off)
    - boto3 com credenciais dummy aceitas em dev
"""

import json
import time
import uuid

import boto3
import pytest


SHIM_ENDPOINT = "http://localhost:4566"
SHIM_REGION = "us-east-1"
DEV_ACCESS_KEY = "AKIADEV0000000000000"
DEV_SECRET_KEY = "dev-secret-key-not-validated"


# --- Fixtures locais ---


@pytest.fixture
def sns_client():
    return boto3.client(
        "sns",
        endpoint_url=SHIM_ENDPOINT,
        region_name=SHIM_REGION,
        aws_access_key_id=DEV_ACCESS_KEY,
        aws_secret_access_key=DEV_SECRET_KEY,
    )


@pytest.fixture
def sqs_client():
    return boto3.client(
        "sqs",
        endpoint_url=SHIM_ENDPOINT,
        region_name=SHIM_REGION,
        aws_access_key_id=DEV_ACCESS_KEY,
        aws_secret_access_key=DEV_SECRET_KEY,
    )


@pytest.fixture
def unique_topic_name():
    return f"test-topic-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def unique_queue_name():
    return f"test-queue-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def clean_topic(sns_client):
    """Yields nome único e limpa o tópico após o teste."""
    name = f"test-topic-{uuid.uuid4().hex[:12]}"
    yield name
    # Cleanup: deleta se existe.
    try:
        listed = sns_client.list_topics()["Topics"]
        for t in listed:
            if t["TopicArn"].endswith(f":{name}"):
                # Remove todas as subscriptions
                subs = sns_client.list_subscriptions_by_topic(TopicArn=t["TopicArn"]).get("Subscriptions", [])
                for s in subs:
                    sns_client.unsubscribe(SubscriptionArn=s["SubscriptionArn"])
                sns_client.delete_topic(TopicArn=t["TopicArn"])
    except Exception:
        pass


@pytest.fixture
def clean_queue(sqs_client):
    """Yields nome único e limpa a fila após o teste."""
    name = f"test-queue-{uuid.uuid4().hex[:12]}"
    yield name
    try:
        sqs_client.delete_queue(QueueUrl=f"{SHIM_ENDPOINT}/000000000000/{name}")
    except Exception:
        pass


# --- Testes de tópicos ---


def test_create_topic_returns_arn(sns_client, unique_topic_name):
    """CreateTopic retorna ARN com formato correto."""
    resp = sns_client.create_topic(Name=unique_topic_name)
    arn = resp["TopicArn"]
    assert arn.startswith("arn:aws:sns:")
    assert unique_topic_name in arn
    sns_client.delete_topic(TopicArn=arn)


def test_create_topic_idempotent(sns_client, unique_topic_name):
    """CreateTopic com mesmo nome retorna o mesmo ARN (idempotente)."""
    arn1 = sns_client.create_topic(Name=unique_topic_name)["TopicArn"]
    arn2 = sns_client.create_topic(Name=unique_topic_name)["TopicArn"]
    assert arn1 == arn2
    sns_client.delete_topic(TopicArn=arn1)


def test_list_topics_includes_created(sns_client, unique_topic_name):
    """ListTopics inclui tópicos criados."""
    created = sns_client.create_topic(Name=unique_topic_name)["TopicArn"]
    listed = sns_client.list_topics()["Topics"]
    arns = [t["TopicArn"] for t in listed]
    assert created in arns
    sns_client.delete_topic(TopicArn=created)


def test_delete_topic_removes_from_list(sns_client, unique_topic_name):
    """DeleteTopic remove o tópico da listagem."""
    arn = sns_client.create_topic(Name=unique_topic_name)["TopicArn"]
    sns_client.delete_topic(TopicArn=arn)
    listed = sns_client.list_topics()["Topics"]
    arns = [t["TopicArn"] for t in listed]
    assert arn not in arns


def test_delete_nonexistent_topic_is_idempotent(sns_client):
    """DeleteTopic de ARN inexistente NÃO deve falhar — AWS é idempotente."""
    fake_arn = "arn:aws:sns:us-east-1:000000000000:does-not-exist-xyz"
    # Não deve levantar exceção.
    sns_client.delete_topic(TopicArn=fake_arn)


def test_publish_to_nonexistent_topic_raises(sns_client):
    """Publish em tópico inexistente deve falhar."""
    fake_arn = "arn:aws:sns:us-east-1:000000000000:does-not-exist-xyz"
    with pytest.raises(Exception):
        sns_client.publish(TopicArn=fake_arn, Message="x")


def test_publish_empty_message_raises(sns_client, clean_topic):
    """Publish sem Message deve falhar com InvalidParameter."""
    arn = sns_client.create_topic(Name=clean_topic)["TopicArn"]
    with pytest.raises(Exception) as exc_info:
        sns_client.publish(TopicArn=arn, Message="")
    assert "Parameter" in str(exc_info.value) or "Invalid" in str(exc_info.value)


def test_publish_too_large_raises(sns_client, clean_topic):
    """Publish com Message > 256 KiB deve falhar."""
    arn = sns_client.create_topic(Name=clean_topic)["TopicArn"]
    big = "x" * (256 * 1024 + 1)
    with pytest.raises(Exception) as exc_info:
        sns_client.publish(TopicArn=arn, Message=big)
    assert "InvalidParameter" in str(exc_info.value) or "256" in str(exc_info.value)


# --- Testes de subscriptions ---


def test_subscribe_sqs_queue_returns_arn(sns_client, sqs_client, clean_topic, clean_queue):
    """Subscribe de uma fila SQS retorna ARN de subscription."""
    sqs_client.create_queue(QueueName=clean_queue)
    queue_url = sqs_client.get_queue_url(QueueName=clean_queue)["QueueUrl"]
    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    topic_arn = sns_client.create_topic(Name=clean_topic)["TopicArn"]
    sub_arn = sns_client.subscribe(
        TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn
    )["SubscriptionArn"]

    assert sub_arn.startswith("arn:aws:sns:")
    assert topic_arn in sub_arn


def test_unsubscribe_removes_subscription(sns_client, sqs_client, clean_topic, clean_queue):
    """Unsubscribe remove a subscription."""
    sqs_client.create_queue(QueueName=clean_queue)
    queue_url = sqs_client.get_queue_url(QueueName=clean_queue)["QueueUrl"]
    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    topic_arn = sns_client.create_topic(Name=clean_topic)["TopicArn"]
    sub_arn = sns_client.subscribe(
        TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn
    )["SubscriptionArn"]

    sns_client.unsubscribe(SubscriptionArn=sub_arn)

    subs = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
    arns = [s["SubscriptionArn"] for s in subs]
    assert sub_arn not in arns


def test_unsubscribe_nonexistent_is_idempotent(sns_client):
    """Unsubscribe de ARN inexistente NÃO deve falhar — AWS é idempotente."""
    fake = "arn:aws:sns:us-east-1:000000000000:nonexistent:0000000000000000"
    sns_client.unsubscribe(SubscriptionArn=fake)


def test_list_subscriptions_by_topic(sns_client, sqs_client, clean_topic, clean_queue):
    """ListSubscriptionsByTopic retorna subscription criada."""
    sqs_client.create_queue(QueueName=clean_queue)
    queue_url = sqs_client.get_queue_url(QueueName=clean_queue)["QueueUrl"]
    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    topic_arn = sns_client.create_topic(Name=clean_topic)["TopicArn"]
    sub_arn = sns_client.subscribe(
        TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn
    )["SubscriptionArn"]

    subs = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
    assert len(subs) == 1
    assert subs[0]["SubscriptionArn"] == sub_arn


def test_list_subscriptions_global(sns_client, sqs_client, clean_topic, clean_queue):
    """ListSubscriptions (sem filtro) inclui a subscription criada."""
    sqs_client.create_queue(QueueName=clean_queue)
    queue_url = sqs_client.get_queue_url(QueueName=clean_queue)["QueueUrl"]
    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    topic_arn = sns_client.create_topic(Name=clean_topic)["TopicArn"]
    sub_arn = sns_client.subscribe(
        TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn
    )["SubscriptionArn"]

    subs = sns_client.list_subscriptions()["Subscriptions"]
    arns = [s["SubscriptionArn"] for s in subs]
    assert sub_arn in arns


# --- Testes de publish + fan-out ---


def test_publish_delivers_to_sqs_subscriber(sns_client, sqs_client, clean_topic, clean_queue):
    """Publish entrega mensagem na fila SQS inscrita."""
    sqs_client.create_queue(QueueName=clean_queue)
    queue_url = sqs_client.get_queue_url(QueueName=clean_queue)["QueueUrl"]
    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    topic_arn = sns_client.create_topic(Name=clean_topic)["TopicArn"]
    sns_client.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

    sns_client.publish(TopicArn=topic_arn, Message="hello-sns")

    msgs = sqs_client.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=5
    )["Messages"]
    assert len(msgs) == 1
    assert msgs[0]["Body"] == "hello-sns"


def test_publish_fanout_multiple_subscribers(sns_client, sqs_client, clean_topic):
    """Publish faz fan-out para múltiplas subscriptions."""
    queues = []
    for i in range(3):
        qn = f"test-fanout-{uuid.uuid4().hex[:8]}"
        sqs_client.create_queue(QueueName=qn)
        qu = sqs_client.get_queue_url(QueueName=qn)["QueueUrl"]
        qarn = sqs_client.get_queue_attributes(
            QueueUrl=qu, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        queues.append((qn, qu, qarn))

    topic_arn = sns_client.create_topic(Name=clean_topic)["TopicArn"]
    for _, _, qarn in queues:
        sns_client.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=qarn)

    # Publica 3 mensagens
    for i in range(3):
        sns_client.publish(TopicArn=topic_arn, Message=f"msg-{i}")

    # Cada queue deve receber as 3
    for qn, qu, _ in queues:
        got = sqs_client.receive_message(
            QueueUrl=qu, MaxNumberOfMessages=10, WaitTimeSeconds=3
        )["Messages"]
        bodies = sorted([m["Body"] for m in got])
        assert bodies == ["msg-0", "msg-1", "msg-2"], f"queue {qn} got {bodies}"
        sqs_client.delete_queue(QueueUrl=qu)


def test_publish_with_message_attributes(sns_client, sqs_client, clean_topic, clean_queue):
    """Publish preserva MessageAttributes na entrega."""
    sqs_client.create_queue(QueueName=clean_queue)
    queue_url = sqs_client.get_queue_url(QueueName=clean_queue)["QueueUrl"]
    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    topic_arn = sns_client.create_topic(Name=clean_topic)["TopicArn"]
    sns_client.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

    sns_client.publish(
        TopicArn=topic_arn,
        Message="with-attrs",
        MessageAttributes={
            "source": {"DataType": "String", "StringValue": "test"},
            "count": {"DataType": "Number", "StringValue": "42"},
        },
    )

    msgs = sqs_client.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=5
    )["Messages"]
    assert len(msgs) == 1
    md = msgs[0].get("MessageAttributes", {})
    assert md.get("source", {}).get("StringValue") == "test"
    assert md.get("count", {}).get("StringValue") == "42"


def test_publish_with_subject(sns_client, sqs_client, clean_topic, clean_queue):
    """Publish com Subject expõe Subject como MessageAttribute."""
    sqs_client.create_queue(QueueName=clean_queue)
    queue_url = sqs_client.get_queue_url(QueueName=clean_queue)["QueueUrl"]
    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    topic_arn = sns_client.create_topic(Name=clean_topic)["TopicArn"]
    sns_client.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

    sns_client.publish(TopicArn=topic_arn, Message="subj-msg", Subject="MySubject")

    msgs = sqs_client.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=5
    )["Messages"]
    assert len(msgs) == 1
    assert msgs[0].get("MessageAttributes", {}).get("Subject", {}).get("StringValue") == "MySubject"


# --- Filter Policy ---


def test_filter_policy_matches(sns_client, sqs_client, clean_topic, clean_queue):
    """FilterPolicy aceita mensagens que casam; rejeita outras."""
    sqs_client.create_queue(QueueName=clean_queue)
    queue_url = sqs_client.get_queue_url(QueueName=clean_queue)["QueueUrl"]
    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    topic_arn = sns_client.create_topic(Name=clean_topic)["TopicArn"]
    sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
        Attributes={"FilterPolicy": '{"type":["alert"]}'},
    )

    for i in range(3):
        sns_client.publish(
            TopicArn=topic_arn,
            Message=f"alert-{i}",
            MessageAttributes={"type": {"DataType": "String", "StringValue": "alert"}},
        )
    for i in range(3):
        sns_client.publish(
            TopicArn=topic_arn,
            Message=f"info-{i}",
            MessageAttributes={"type": {"DataType": "String", "StringValue": "info"}},
        )

    msgs = sqs_client.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=5
    )["Messages"]
    bodies = sorted([m["Body"] for m in msgs])
    assert bodies == ["alert-0", "alert-1", "alert-2"]


def test_filter_policy_no_filter_delivers_all(sns_client, sqs_client, clean_topic, clean_queue):
    """Sem FilterPolicy, todas as mensagens passam."""
    sqs_client.create_queue(QueueName=clean_queue)
    queue_url = sqs_client.get_queue_url(QueueName=clean_queue)["QueueUrl"]
    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    topic_arn = sns_client.create_topic(Name=clean_topic)["TopicArn"]
    sns_client.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

    for i in range(3):
        sns_client.publish(TopicArn=topic_arn, Message=f"m-{i}")

    msgs = sqs_client.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=5
    )["Messages"]
    assert len(msgs) == 3


# --- DeleteTopic cascade ---


def test_delete_topic_cascades_subscriptions(sns_client, sqs_client, clean_topic, clean_queue):
    """DeleteTopic deve remover todas as subscriptions órfãs."""
    sqs_client.create_queue(QueueName=clean_queue)
    queue_url = sqs_client.get_queue_url(QueueName=clean_queue)["QueueUrl"]
    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    topic_arn = sns_client.create_topic(Name=clean_topic)["TopicArn"]
    sns_client.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

    sns_client.delete_topic(TopicArn=topic_arn)

    subs = sns_client.list_subscriptions()["Subscriptions"]
    # Não deve haver subscription órfã deste tópico.
    for s in subs:
        assert not s["SubscriptionArn"].startswith(topic_arn)

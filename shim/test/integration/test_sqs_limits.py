"""
test_sqs_limits.py — testes E2E dos limites e semânticas de produção.

Cobre:
  - Limite de 10 MessageAttributes por mensagem (AWS).
  - Atributos contam no limite de 256 KiB (não são bypass).
  - ApproximateReceiveCount real (incrementa a cada redelivery).
  - ApproximateNumberOfMessages diminui após delete (retention WorkQueue:
    mensagem consumida é apagada, não fica em disco até MaxAge).
"""

import time

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def queue(sqs_client, unique_queue_name):
    """Cria fila e garante cleanup."""
    resp = sqs_client.create_queue(QueueName=unique_queue_name)
    q = {"QueueUrl": resp["QueueUrl"], "QueueName": unique_queue_name}
    yield q
    try:
        sqs_client.delete_queue(QueueUrl=q["QueueUrl"])
    except Exception:
        pass


def _attrs(n):
    return {
        f"attr{i}": {"DataType": "String", "StringValue": f"v{i}"}
        for i in range(n)
    }


def test_send_message_max_10_attributes(queue, sqs_client):
    """Até 10 message attributes: aceito. 11: InvalidParameterValue."""
    resp = sqs_client.send_message(
        QueueUrl=queue["QueueUrl"],
        MessageBody="ok",
        MessageAttributes=_attrs(10),
    )
    assert "MessageId" in resp

    with pytest.raises(ClientError) as exc:
        sqs_client.send_message(
            QueueUrl=queue["QueueUrl"],
            MessageBody="too-many",
            MessageAttributes=_attrs(11),
        )
    assert exc.value.response["Error"]["Code"].startswith("InvalidParameterValue")


def test_send_message_batch_rejects_entry_with_too_many_attributes(queue, sqs_client):
    """Batch com entry >10 atributos é rejeitado com InvalidParameterValue."""
    with pytest.raises(ClientError) as exc:
        sqs_client.send_message_batch(
            QueueUrl=queue["QueueUrl"],
            Entries=[
                {"Id": "ok", "MessageBody": "a"},
                {"Id": "bad", "MessageBody": "b", "MessageAttributes": _attrs(11)},
            ],
        )
    assert exc.value.response["Error"]["Code"].startswith("InvalidParameterValue")


def test_attributes_count_toward_message_size_limit(queue, sqs_client):
    """Atributos contam no limite de 256 KiB — não são bypass do limite.

    Body sozinho cabe (250 KiB < 256 KiB), mas body + atributo de 10 KiB
    estoura. AWS rejeita; sem essa validação, atributos permitiriam
    payloads arbitrariamente grandes.
    """
    body = "x" * (250 * 1024)
    big_attr = {
        "payload": {"DataType": "String", "StringValue": "y" * (10 * 1024)},
    }
    with pytest.raises(ClientError) as exc:
        sqs_client.send_message(
            QueueUrl=queue["QueueUrl"],
            MessageBody=body,
            MessageAttributes=big_attr,
        )
    code = exc.value.response["Error"]["Code"]
    assert code.startswith(("MessageTooLarge", "InvalidParameterValue"))

    # Sanity: o mesmo body SEM o atributo grande é aceito.
    resp = sqs_client.send_message(QueueUrl=queue["QueueUrl"], MessageBody=body)
    assert "MessageId" in resp


def test_approximate_receive_count_increments(queue, sqs_client):
    """ApproximateReceiveCount reflete redeliveries reais (NumDelivered).

    1ª entrega → "1". Após ChangeMessageVisibility(0) + novo receive,
    a redelivery → "2". Antes era hardcoded "1" — dead-letter logic de
    clientes (maxReceiveCount) nunca dispararia.
    """
    sqs_client.send_message(QueueUrl=queue["QueueUrl"], MessageBody="rc-test")

    rx1 = sqs_client.receive_message(
        QueueUrl=queue["QueueUrl"],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=1,
        AttributeNames=["All"],
    )
    m1 = rx1["Messages"][0]
    assert m1["Attributes"]["ApproximateReceiveCount"] == "1"

    # Volta a mensagem para a fila imediatamente.
    sqs_client.change_message_visibility(
        QueueUrl=queue["QueueUrl"],
        ReceiptHandle=m1["ReceiptHandle"],
        VisibilityTimeout=0,
    )

    rx2 = sqs_client.receive_message(
        QueueUrl=queue["QueueUrl"],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=2,
        AttributeNames=["All"],
    )
    m2 = rx2["Messages"][0]
    assert int(m2["Attributes"]["ApproximateReceiveCount"]) >= 2


def _queue_depth(sqs_client, url):
    resp = sqs_client.get_queue_attributes(
        QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"]
    )
    return int(resp["Attributes"]["ApproximateNumberOfMessages"])


def test_queue_depth_drops_after_delete(queue, sqs_client):
    """Mensagem deletada some do stream (WorkQueue retention).

    Com LimitsPolicy (antes), mensagens acked ficavam em disco até MaxAge
    e ApproximateNumberOfMessages nunca diminuía. Com WorkQueuePolicy,
    o ack apaga a mensagem: depth volta a 0 e o custo de disco é
    proporcional ao backlog, não ao throughput histórico.
    """
    url = queue["QueueUrl"]
    for i in range(3):
        sqs_client.send_message(QueueUrl=url, MessageBody=f"m{i}")

    assert _queue_depth(sqs_client, url) == 3

    received = 0
    deadline = time.time() + 10
    while received < 3 and time.time() < deadline:
        rx = sqs_client.receive_message(
            QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1
        )
        for m in rx.get("Messages", []):
            sqs_client.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
            received += 1
    assert received == 3

    # Depth deve cair para 0 (poll com tolerância a lag de update).
    deadline = time.time() + 5
    depth = None
    while time.time() < deadline:
        depth = _queue_depth(sqs_client, url)
        if depth == 0:
            break
        time.sleep(0.3)
    assert depth == 0

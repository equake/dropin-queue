"""
test_sqs_batch.py — testes E2E para SendMessageBatch e DeleteMessageBatch.

Os testes rodam contra o shim rodando via docker-compose em http://localhost:4566
com NATS JetStream 2.10. Assume AUTH_MODE=off (dev).

Cobertura:

SendMessageBatch:
- round-trip: batch com N entries → todas em Successful, receive devolve N msgs
- partial failure: batch com 1 entry vazia (MessageBody ausente) → 1 Failed
- validação de limite: 11 entries → TooManyEntriesInBatch
- validação de Ids únicos: 2 entries com mesmo Id → BatchEntryIdsNotDistinct
- validação de Ids vazios: 1 entry com Id ausente → InvalidParameterValue
- FIFO: batch em fila .fifo sem MessageGroupId em uma entry → Failed[]
- FIFO: batch em fila .fifo com MessageGroupId em todas → Successful com SequenceNumber
- atributos: MessageAttributes (String) preservados per-entry

DeleteMessageBatch:
- round-trip: receive 5 msgs → delete batch → fila vazia
- partial failure: 2 entries válidas + 1 inválida (ReceiptHandle malformado) → 2 Successful + 1 Failed
- validação de limite: 11 entries → TooManyEntriesInBatch
- validação de Ids únicos: 2 entries com mesmo Id → BatchEntryIdsNotDistinct
- erro quando queue não existe → QueueDoesNotExist
"""

import pytest

from botocore.exceptions import ClientError


def _code(exc: ClientError) -> str:
    """Extrai o error code de um ClientError, lidando com o suffix 'Exception'."""
    err = exc.response.get("Error", {}) if hasattr(exc, "response") else {}
    return err.get("Code", "")


# --- SendMessageBatch ---

def test_send_message_batch_round_trip(sqs_client, unique_queue_name):
    """Batch com 5 entries → todas bem-sucedidas, receive devolve 5."""
    qname = unique_queue_name
    sqs_client.create_queue(QueueName=qname)
    url = sqs_client.get_queue_url(QueueName=qname)["QueueUrl"]

    entries = [
        {"Id": f"id-{i}", "MessageBody": f"body-{i}"}
        for i in range(5)
    ]
    resp = sqs_client.send_message_batch(QueueUrl=url, Entries=entries)

    assert len(resp["Successful"]) == 5
    assert len(resp["Failed"]) == 0
    successful_ids = {e["Id"] for e in resp["Successful"]}
    assert successful_ids == {f"id-{i}" for i in range(5)}
    # Cada entry bem-sucedida tem MessageId.
    for entry in resp["Successful"]:
        assert "MessageId" in entry
        assert entry["MessageId"]

    # Receive confirma que todas estão na fila.
    got = sqs_client.receive_message(
        QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=0
    )["Messages"]
    assert len(got) == 5
    bodies = sorted(m["Body"] for m in got)
    assert bodies == [f"body-{i}" for i in range(5)]


def test_send_message_batch_partial_failure_empty_body(sqs_client, unique_queue_name):
    """Batch com 1 entry com MessageBody vazia → Mixed Successful+Failed."""
    qname = unique_queue_name
    sqs_client.create_queue(QueueName=qname)
    url = sqs_client.get_queue_url(QueueName=qname)["QueueUrl"]

    entries = [
        {"Id": "ok", "MessageBody": "valid"},
        {"Id": "bad", "MessageBody": ""},
        {"Id": "ok2", "MessageBody": "also-valid"},
    ]
    resp = sqs_client.send_message_batch(QueueUrl=url, Entries=entries)

    assert len(resp["Successful"]) == 2
    assert len(resp["Failed"]) == 1
    assert resp["Failed"][0]["Id"] == "bad"
    assert resp["Failed"][0]["Code"] == "MissingParameter"
    assert resp["Failed"][0]["SenderFault"] is True


def test_send_message_batch_too_many_entries(sqs_client, unique_queue_name):
    """Batch com 11 entries → TooManyEntriesInBatch (limite AWS é 10)."""
    qname = unique_queue_name
    sqs_client.create_queue(QueueName=qname)
    url = sqs_client.get_queue_url(QueueName=qname)["QueueUrl"]

    entries = [
        {"Id": f"id-{i}", "MessageBody": f"body-{i}"}
        for i in range(11)
    ]
    with pytest.raises(ClientError) as exc:
        sqs_client.send_message_batch(QueueUrl=url, Entries=entries)
    assert "TooManyEntriesInBatch" in _code(exc.value)


def test_send_message_batch_duplicate_ids(sqs_client, unique_queue_name):
    """Batch com 2 entries com mesmo Id → BatchEntryIdsNotDistinct."""
    qname = unique_queue_name
    sqs_client.create_queue(QueueName=qname)
    url = sqs_client.get_queue_url(QueueName=qname)["QueueUrl"]

    entries = [
        {"Id": "dup", "MessageBody": "first"},
        {"Id": "dup", "MessageBody": "second"},
        {"Id": "ok", "MessageBody": "third"},
    ]
    with pytest.raises(ClientError) as exc:
        sqs_client.send_message_batch(QueueUrl=url, Entries=entries)
    assert "BatchEntryIdsNotDistinct" in _code(exc.value)


def test_send_message_batch_empty_ids(sqs_client, unique_queue_name):
    """Batch com Id vazio em uma entry → InvalidParameterValue."""
    qname = unique_queue_name
    sqs_client.create_queue(QueueName=qname)
    url = sqs_client.get_queue_url(QueueName=qname)["QueueUrl"]

    entries = [
        {"Id": "ok", "MessageBody": "first"},
        {"Id": "", "MessageBody": "second"},
    ]
    with pytest.raises(ClientError) as exc:
        sqs_client.send_message_batch(QueueUrl=url, Entries=entries)
    assert "InvalidParameterValue" in _code(exc.value)


def test_send_message_batch_fifo_requires_group_id(sqs_client, unique_queue_name):
    """Batch em fila .fifo sem MessageGroupId em uma entry → Failed per-entry."""
    qname = unique_queue_name + ".fifo"
    sqs_client.create_queue(
        QueueName=qname,
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )
    url = sqs_client.get_queue_url(QueueName=qname)["QueueUrl"]

    entries = [
        {"Id": "ok", "MessageBody": "first", "MessageGroupId": "g1"},
        {"Id": "bad", "MessageBody": "second"},  # sem MessageGroupId
    ]
    resp = sqs_client.send_message_batch(QueueUrl=url, Entries=entries)

    assert len(resp["Successful"]) == 1
    assert len(resp["Failed"]) == 1
    assert resp["Failed"][0]["Id"] == "bad"
    assert resp["Failed"][0]["Code"] == "MissingParameter"
    # Entry FIFO bem-sucedida tem SequenceNumber.
    assert "SequenceNumber" in resp["Successful"][0]


def test_send_message_batch_fifo_with_group_id_and_dedup(sqs_client, unique_queue_name):
    """Batch em fila .fifo com MessageGroupId e DeduplicationId → todas com SequenceNumber."""
    qname = unique_queue_name + ".fifo"
    sqs_client.create_queue(
        QueueName=qname,
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )
    url = sqs_client.get_queue_url(QueueName=qname)["QueueUrl"]

    entries = [
        {
            "Id": f"id-{i}",
            "MessageBody": f"body-{i}",
            "MessageGroupId": "group-a",
            "MessageDeduplicationId": f"dedup-{i}",
        }
        for i in range(3)
    ]
    resp = sqs_client.send_message_batch(QueueUrl=url, Entries=entries)

    assert len(resp["Successful"]) == 3
    assert len(resp["Failed"]) == 0
    seqs = [e["SequenceNumber"] for e in resp["Successful"]]
    # SequenceNumbers devem ser únicos e em ordem crescente (preservação FIFO).
    assert len(set(seqs)) == 3
    assert sorted(seqs) == seqs


def test_send_message_batch_with_attributes(sqs_client, unique_queue_name):
    """Batch com MessageAttributes per-entry → preservados no ReceiveMessage."""
    qname = unique_queue_name
    sqs_client.create_queue(QueueName=qname)
    url = sqs_client.get_queue_url(QueueName=qname)["QueueUrl"]

    entries = [
        {
            "Id": "attr1",
            "MessageBody": "with-attr",
            "MessageAttributes": {
                "k": {"DataType": "String", "StringValue": "v1"},
            },
        },
        {
            "Id": "attr2",
            "MessageBody": "no-attr",
        },
    ]
    resp = sqs_client.send_message_batch(QueueUrl=url, Entries=entries)
    assert len(resp["Successful"]) == 2

    got = sqs_client.receive_message(
        QueueUrl=url, MaxNumberOfMessages=10, MessageAttributeNames=["All"],
        WaitTimeSeconds=0,
    )["Messages"]
    assert len(got) == 2

    # Encontra msg com atributo.
    by_body = {m["Body"]: m for m in got}
    assert "k" in by_body["with-attr"]["MessageAttributes"]
    assert by_body["with-attr"]["MessageAttributes"]["k"]["StringValue"] == "v1"
    assert "k" not in by_body["no-attr"]["MessageAttributes"]


# --- DeleteMessageBatch ---

def test_delete_message_batch_round_trip(sqs_client, unique_queue_name):
    """Send 5 msgs, receive 5, delete batch → fila vazia."""
    qname = unique_queue_name
    sqs_client.create_queue(QueueName=qname)
    url = sqs_client.get_queue_url(QueueName=qname)["QueueUrl"]

    sqs_client.send_message_batch(
        QueueUrl=url,
        Entries=[{"Id": f"id-{i}", "MessageBody": f"body-{i}"} for i in range(5)],
    )
    got = sqs_client.receive_message(
        QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=0,
    )["Messages"]
    assert len(got) == 5

    entries = [
        {"Id": m["MessageId"][:8], "ReceiptHandle": m["ReceiptHandle"]}
        for m in got
    ]
    resp = sqs_client.delete_message_batch(QueueUrl=url, Entries=entries)
    assert len(resp["Successful"]) == 5
    assert len(resp["Failed"]) == 0

    # Fila agora vazia.
    got2 = sqs_client.receive_message(
        QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=0,
    )["Messages"]
    assert got2 == []


def test_delete_message_batch_partial_failure_invalid_handle(sqs_client, unique_queue_name):
    """Batch com ReceiptHandle malformado → 1 Failed com ReceiptHandleIsInvalid."""
    qname = unique_queue_name
    sqs_client.create_queue(QueueName=qname)
    url = sqs_client.get_queue_url(QueueName=qname)["QueueUrl"]

    sqs_client.send_message_batch(
        QueueUrl=url,
        Entries=[
            {"Id": "a", "MessageBody": "first"},
            {"Id": "b", "MessageBody": "second"},
        ],
    )
    got = sqs_client.receive_message(
        QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=0,
    )["Messages"]
    handles = [m["ReceiptHandle"] for m in got]

    entries = [
        {"Id": "ok-a", "ReceiptHandle": handles[0]},
        {"Id": "bad", "ReceiptHandle": "rh1:invalid:999"},
        {"Id": "ok-b", "ReceiptHandle": handles[1]},
    ]
    resp = sqs_client.delete_message_batch(QueueUrl=url, Entries=entries)

    assert len(resp["Successful"]) == 2
    assert len(resp["Failed"]) == 1
    assert resp["Failed"][0]["Id"] == "bad"
    assert resp["Failed"][0]["Code"] == "ReceiptHandleIsInvalid"
    assert resp["Failed"][0]["SenderFault"] is False  # msg já processada


def test_delete_message_batch_too_many_entries(sqs_client, unique_queue_name):
    """DeleteMessageBatch com 11 entries → TooManyEntriesInBatch."""
    qname = unique_queue_name
    sqs_client.create_queue(QueueName=qname)
    url = sqs_client.get_queue_url(QueueName=qname)["QueueUrl"]

    entries = [
        {"Id": f"id-{i}", "ReceiptHandle": "rh1:x:1"} for i in range(11)
    ]
    with pytest.raises(ClientError) as exc:
        sqs_client.delete_message_batch(QueueUrl=url, Entries=entries)
    assert "TooManyEntriesInBatch" in _code(exc.value)


def test_delete_message_batch_duplicate_ids(sqs_client, unique_queue_name):
    """DeleteMessageBatch com Ids duplicados → BatchEntryIdsNotDistinct."""
    qname = unique_queue_name
    sqs_client.create_queue(QueueName=qname)
    url = sqs_client.get_queue_url(QueueName=qname)["QueueUrl"]

    entries = [
        {"Id": "dup", "ReceiptHandle": "rh1:x:1"},
        {"Id": "dup", "ReceiptHandle": "rh1:y:2"},
    ]
    with pytest.raises(ClientError) as exc:
        sqs_client.delete_message_batch(QueueUrl=url, Entries=entries)
    assert "BatchEntryIdsNotDistinct" in _code(exc.value)


def test_delete_message_batch_nonexistent_queue(sqs_client, unique_queue_name):
    """DeleteMessageBatch em fila inexistente → QueueDoesNotExist."""
    url = f"http://localhost:4566/000000000000/{unique_queue_name}-missing"
    entries = [{"Id": "a", "ReceiptHandle": "rh1:x:1"}]

    with pytest.raises(ClientError) as exc:
        sqs_client.delete_message_batch(QueueUrl=url, Entries=entries)
    assert "QueueDoesNotExist" in _code(exc.value)


def test_send_message_batch_nonexistent_queue(sqs_client, unique_queue_name):
    """SendMessageBatch em fila inexistente → QueueDoesNotExist."""
    url = f"http://localhost:4566/000000000000/{unique_queue_name}-missing"
    entries = [{"Id": "a", "MessageBody": "x"}]

    with pytest.raises(ClientError) as exc:
        sqs_client.send_message_batch(QueueUrl=url, Entries=entries)
    assert "QueueDoesNotExist" in _code(exc.value)

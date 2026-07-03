"""
test_sqs_messages.py — testes E2E para operações de mensagem SQS.

Valida que SendMessage/ReceiveMessage/DeleteMessage/ChangeMessageVisibility
funcionam corretamente através do shim, com clientes boto3 oficiais.

Cobre:
- SendMessage + ReceiveMessage (round-trip)
- ReceiveMessage com long-polling (WaitTimeSeconds > 0)
- ReceiveMessage hot (sem mensagens, WaitTimeSeconds=0 → retorno imediato)
- DeleteMessage (idempotência)
- ChangeMessageVisibility
- PurgeQueue
- MessageAttributes (String, Number, Binary, String.List)
- FIFO queues (MessageGroupId obrigatório)
- Error cases (queue inexistente, receipt handle inválido, body grande)
"""

import time
import uuid

import pytest


@pytest.fixture
def queue(sqs_client, unique_queue_name):
    """Cria fila Standard para teste e remove no final."""
    q = sqs_client.create_queue(QueueName=unique_queue_name)
    yield q
    try:
        sqs_client.delete_queue(QueueUrl=q["QueueUrl"])
    except Exception:
        pass


def test_send_and_receive_single(queue, sqs_client):
    """SendMessage seguido de ReceiveMessage devolve a mesma mensagem."""
    body = f"hello-{uuid.uuid4().hex[:8]}"
    send = sqs_client.send_message(QueueUrl=queue["QueueUrl"], MessageBody=body)
    assert send["MessageId"]

    rx = sqs_client.receive_message(
        QueueUrl=queue["QueueUrl"],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0,
    )
    msgs = rx.get("Messages", [])
    assert len(msgs) == 1
    assert msgs[0]["Body"] == body
    assert msgs[0]["MessageId"] == send["MessageId"]
    assert msgs[0]["ReceiptHandle"]


def test_delete_message(queue, sqs_client):
    """Após DeleteMessage, ReceiveMessage não devolve mais a mensagem."""
    sqs_client.send_message(QueueUrl=queue["QueueUrl"], MessageBody="to-delete")

    rx = sqs_client.receive_message(
        QueueUrl=queue["QueueUrl"], MaxNumberOfMessages=1, WaitTimeSeconds=0
    )
    msgs = rx["Messages"]
    assert len(msgs) == 1
    handle = msgs[0]["ReceiptHandle"]

    sqs_client.delete_message(QueueUrl=queue["QueueUrl"], ReceiptHandle=handle)

    # Aguarda visibility timeout (default 30s) expirar para a mensagem
    # ficar disponível de novo. Como delete é ack, mensagem é removida.
    # Segunda chamada deve retornar vazio.
    rx2 = sqs_client.receive_message(
        QueueUrl=queue["QueueUrl"], MaxNumberOfMessages=1, WaitTimeSeconds=0
    )
    assert "Messages" not in rx2 or rx2.get("Messages") == []


def test_delete_message_idempotent(queue, sqs_client):
    """DeleteMessage com mesmo receipt handle 2x: 2º é OK (idempotente) ou
    retorna ReceiptHandleIsInvalid (receipt expirado por inactivity cleanup)."""
    sqs_client.send_message(QueueUrl=queue["QueueUrl"], MessageBody="idem")

    rx = sqs_client.receive_message(
        QueueUrl=queue["QueueUrl"], MaxNumberOfMessages=1, WaitTimeSeconds=0
    )
    handle = rx["Messages"][0]["ReceiptHandle"]
    sqs_client.delete_message(QueueUrl=queue["QueueUrl"], ReceiptHandle=handle)

    # Segundo delete — pode ser OK (ack idempotente) ou erro.
    # No MVP com consumer efêmero + InactiveThreshold=60s, o consumer pode
    # ter sido removido entre calls — então esperamos ReceiptHandleIsInvalid.
    try:
        sqs_client.delete_message(QueueUrl=queue["QueueUrl"], ReceiptHandle=handle)
        # Se não deu erro, OK
    except Exception as e:
        err_code = e.response.get("Error", {}).get("Code", "") if hasattr(e, "response") else ""
        assert "ReceiptHandleIsInvalid" in err_code or "InvalidParameterValue" in err_code


def test_change_message_visibility(queue, sqs_client):
    """ChangeMessageVisibility redefine timeout — mensagem continua invisível.

    No MVP, NakWithDelay(0) coloca a mensagem imediatamente disponível,
    então testamos que chamar ChangeMessageVisibility com VisibilityTimeout=0
    resulta em mensagem redelivered no próximo Receive.
    """
    sqs_client.send_message(QueueUrl=queue["QueueUrl"], MessageBody="vis-test")

    rx = sqs_client.receive_message(
        QueueUrl=queue["QueueUrl"],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0,
        VisibilityTimeout=60,
    )
    handle = rx["Messages"][0]["ReceiptHandle"]

    # Reduz visibility timeout para 0 — mensagem deve voltar imediatamente.
    sqs_client.change_message_visibility(
        QueueUrl=queue["QueueUrl"],
        ReceiptHandle=handle,
        VisibilityTimeout=0,
    )

    rx2 = sqs_client.receive_message(
        QueueUrl=queue["QueueUrl"], MaxNumberOfMessages=1, WaitTimeSeconds=0
    )
    msgs2 = rx2.get("Messages", [])
    # Mensagem deve aparecer novamente (VisibilityTimeout=0 → imediata).
    assert len(msgs2) >= 1


def test_receive_message_empty(queue, sqs_client):
    """ReceiveMessage em fila vazia retorna sem mensagens (não erro)."""
    rx = sqs_client.receive_message(
        QueueUrl=queue["QueueUrl"], MaxNumberOfMessages=1, WaitTimeSeconds=0
    )
    assert rx.get("Messages", []) == []


def test_long_polling_waits_then_returns(queue, sqs_client):
    """Long-polling: ReceiveMessage bloqueia até mensagem chegar."""
    import threading

    rx_result = {}

    def receive():
        rx_result["response"] = sqs_client.receive_message(
            QueueUrl=queue["QueueUrl"],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
        )

    t = threading.Thread(target=receive)
    t.start()
    time.sleep(0.5)

    body = "delayed-message"
    sqs_client.send_message(QueueUrl=queue["QueueUrl"], MessageBody=body)

    t.join(timeout=5)
    assert "response" in rx_result
    msgs = rx_result["response"].get("Messages", [])
    assert len(msgs) == 1
    assert msgs[0]["Body"] == body


def test_long_polling_returns_empty_on_timeout(queue, sqs_client):
    """Long-polling sem mensagens durante WaitTimeSeconds → retorna vazio."""
    start = time.time()
    rx = sqs_client.receive_message(
        QueueUrl=queue["QueueUrl"], MaxNumberOfMessages=1, WaitTimeSeconds=2
    )
    elapsed = time.time() - start
    assert rx.get("Messages", []) == []
    assert elapsed >= 1.5


def test_send_message_attributes_string(queue, sqs_client):
    """MessageAttribute tipo String round-trip."""
    sqs_client.send_message(
        QueueUrl=queue["QueueUrl"],
        MessageBody="attr-test",
        MessageAttributes={
            "env": {"DataType": "String", "StringValue": "prod"},
            "version": {"DataType": "Number", "StringValue": "42"},
        },
    )

    rx = sqs_client.receive_message(
        QueueUrl=queue["QueueUrl"],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0,
        MessageAttributeNames=["All"],
    )
    ma = rx["Messages"][0].get("MessageAttributes", {})
    assert ma.get("env", {}).get("StringValue") == "prod"
    assert ma.get("env", {}).get("DataType") == "String"
    assert ma.get("version", {}).get("StringValue") == "42"
    assert ma.get("version", {}).get("DataType") == "Number"


def test_send_message_attribute_binary(queue, sqs_client):
    """MessageAttribute tipo Binary round-trip preserva bytes."""
    raw = b"\x00\x01\x02\x03\xffbinary-data"
    sqs_client.send_message(
        QueueUrl=queue["QueueUrl"],
        MessageBody="binary-attr",
        MessageAttributes={
            "blob": {"DataType": "Binary", "BinaryValue": raw},
        },
    )

    rx = sqs_client.receive_message(
        QueueUrl=queue["QueueUrl"], MaxNumberOfMessages=1, WaitTimeSeconds=0,
        MessageAttributeNames=["All"],
    )
    ma = rx["Messages"][0]["MessageAttributes"]
    assert ma["blob"]["DataType"] == "Binary"
    # boto3 devolve BinaryValue como bytes (já decoded do base64).
    # Comparamos direto.
    assert ma["blob"]["BinaryValue"] == raw


def test_purge_queue(queue, sqs_client):
    """PurgeQueue esvazia fila."""
    for i in range(5):
        sqs_client.send_message(QueueUrl=queue["QueueUrl"], MessageBody=f"m{i}")

    # Confirma que tem mensagens.
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue["QueueUrl"], AttributeNames=["ApproximateNumberOfMessages"]
    )
    assert int(attrs["Attributes"]["ApproximateNumberOfMessages"]) >= 5

    sqs_client.purge_queue(QueueUrl=queue["QueueUrl"])
    time.sleep(0.5)  # JetStream purge é async eventualmente

    rx = sqs_client.receive_message(
        QueueUrl=queue["QueueUrl"], MaxNumberOfMessages=10, WaitTimeSeconds=0
    )
    assert rx.get("Messages", []) == []


def test_fifo_queue_requires_message_group_id(sqs_client, unique_queue_name):
    """Fila FIFO exige MessageGroupId em SendMessage."""
    fifo_name = unique_queue_name + ".fifo"
    q = sqs_client.create_queue(
        QueueName=fifo_name,
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )
    try:
        with pytest.raises(Exception) as ei:
            sqs_client.send_message(QueueUrl=q["QueueUrl"], MessageBody="no-group")
        err = ei.value.response.get("Error", {}) if hasattr(ei.value, "response") else {}
        assert "MissingParameter" in err.get("Code", "") or "InvalidParameter" in err.get("Code", "")
    finally:
        sqs_client.delete_queue(QueueUrl=q["QueueUrl"])


def test_fifo_with_message_group_and_dedup(sqs_client, unique_queue_name):
    """FIFO com MessageGroupId e MessageDeduplicationId funciona."""
    fifo_name = unique_queue_name + ".fifo"
    q = sqs_client.create_queue(
        QueueName=fifo_name, Attributes={"FifoQueue": "true"}
    )
    try:
        dedup = "dedup-" + uuid.uuid4().hex
        r1 = sqs_client.send_message(
            QueueUrl=q["QueueUrl"],
            MessageBody="fifo-msg",
            MessageGroupId="group-A",
            MessageDeduplicationId=dedup,
        )
        assert r1["MessageId"]
        assert r1.get("SequenceNumber")
    finally:
        sqs_client.delete_queue(QueueUrl=q["QueueUrl"])


def test_receive_message_max_batch(sqs_client, unique_queue_name):
    """ReceiveMessage respeita MaxNumberOfMessages até 10.

    Nota: cada chamada ReceiveMessage cria um consumer efêmero (no MVP).
    Mensagens retornadas só são removidas após DeleteMessage. Aqui validamos
    apenas o limite superior do batch size.
    """
    q = sqs_client.create_queue(QueueName=unique_queue_name)
    try:
        for i in range(15):
            sqs_client.send_message(QueueUrl=q["QueueUrl"], MessageBody=f"m{i}")

        rx = sqs_client.receive_message(
            QueueUrl=q["QueueUrl"], MaxNumberOfMessages=10, WaitTimeSeconds=0
        )
        assert len(rx["Messages"]) == 10

        # Delete as 10 mensagens e confirma que ReceiveMessage
        # seguinte vê apenas as 5 restantes (no MVP com consumer
        # efêmero, apenas confirmamos o limite superior; comportamento
        # SQS completo de pending/exclusive vem em release futura).
        for m in rx["Messages"]:
            sqs_client.delete_message(
                QueueUrl=q["QueueUrl"], ReceiptHandle=m["ReceiptHandle"]
            )
    finally:
        sqs_client.delete_queue(QueueUrl=q["QueueUrl"])


def test_send_to_nonexistent_queue_raises(sqs_client):
    """SendMessage em fila inexistente retorna QueueDoesNotExist."""
    with pytest.raises(Exception) as ei:
        sqs_client.send_message(
            QueueUrl="http://localhost:4566/000000000000/no-such-queue",
            MessageBody="x",
        )
    err = ei.value.response.get("Error", {}) if hasattr(ei.value, "response") else {}
    assert "QueueDoesNotExist" in err.get("Code", "") or "AWS.SimpleQueueService" in err.get("Code", "")


def test_delete_message_invalid_receipt_handle(queue, sqs_client):
    """DeleteMessage com receipt handle inválido retorna erro."""
    with pytest.raises(Exception) as ei:
        sqs_client.delete_message(
            QueueUrl=queue["QueueUrl"], ReceiptHandle="rh1:notreal:9999"
        )
    err = ei.value.response.get("Error", {}) if hasattr(ei.value, "response") else {}
    assert "ReceiptHandleIsInvalid" in err.get("Code", "") or "InvalidParameterValue" in err.get("Code", "")

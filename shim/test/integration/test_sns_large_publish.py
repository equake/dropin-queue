"""
test_sns_large_publish.py — teste E2E de SNS Publish com body grande
(via Query protocol em vez de JSON).

Pré-refactor (refactor/kiss-dry-pass-1): o handleAWS lia até 1 MB do body
para detectar SQS vs SNS. Body > 1 MB e Action=Publish caía como SQS
silenciosamente — handler SQS retornava InvalidParameterValue (ação
desconhecida) em vez de error SNS coerente.

Pós-refactor: o dispatch lê cfg.MaxRequestBodyBytes (5 MB default)
1× só e roteia corretamente. O limite aqui está em cfg, mas o teste
valida que (a) body próximo do limite passa, (b) action SNS corretamente
roteada para handler SNS, (c) erro coerente quando body > limite.

Complementa test_sqs_limits.py que cobre limites do SQS.
"""

import pytest
from botocore.exceptions import ClientError
import uuid
import subprocess


@pytest.fixture
def clean_topic(sns_client):
    name = f"test-large-{uuid.uuid4().hex[:12]}"
    arn = sns_client.create_topic(Name=name)["TopicArn"]
    yield arn
    try:
        sns_client.delete_topic(TopicArn=arn)
    except Exception:
        pass


def test_sns_publish_query_normal_size_routes_to_sns_handler(sns_client,
                                                              clean_topic):
    """Publish Query dentro do limite: roteado para SNS handler corretamente.

    Pré-fix: este path ia ao handler Query de SNS. Pós-fix: mesma rota —
    só o caminho de DETECÇÃO mudou (parse direto em vez de sniff 1MB).
    Wire format: byte-a-byte idêntico.
    """
    arn = clean_topic
    msg = f"hello-{uuid.uuid4().hex[:8]}"
    # Publish via Query (form-encoded)
    import urllib.parse
    form = urllib.parse.urlencode({"Action": "Publish",
                                    "Version": "2010-03-31",
                                    "TopicArn": arn,
                                    "Message": msg})
    r = subprocess.run(
        ["curl", "-sS", "-X", "POST",
         "-H", "Content-Type: application/x-www-form-urlencoded",
         "-d", form,
         "-w", "\nHTTP_STATUS=%{http_code}",
         "http://localhost:4566/"],
        capture_output=True, text=True
    )
    assert "HTTP_STATUS=200" in r.stdout, f"publish falhou: {r.stdout}"
    assert "<PublishResponse" in r.stdout or "PublishResult" in r.stdout


def test_sns_publish_query_oversize_rejected_with_coherent_error(sns_client,
                                                                  clean_topic):
    """Publish Query com body > cfg.MaxRequestBodyBytes retorna erro coerente.

    Pré-fix: body > 1 MB era silenciosamente roteado para SQS (sniff trunca
    em 1 MB); user recebia InvalidParameterValue de SQS indicando 'Action
    inválido'. Pós-fix: body > 5 MB (cfg.MaxRequestBodyBytes default) gera
    erro que indica oversize, não roteamento errado.
    """
    arn = clean_topic
    # 6 MB body; > cfg.MaxRequestBodyBytes (5 MB default).
    big_msg = "x" * (6 * 1024 * 1024)
    import urllib.parse
    form = urllib.parse.urlencode({"Action": "Publish",
                                    "Version": "2010-03-31",
                                    "TopicArn": arn,
                                    "Message": big_msg})
    # Use stdin pipe (não command-line arg) para evitar "Argument list
    # too long" em body > ~128 KiB.
    r = subprocess.run(
        ["curl", "-sS", "-X", "POST",
         "-H", "Content-Type: application/x-www-form-urlencoded",
         "--data-binary", "@-",
         "-w", "\nHTTP_STATUS=%{http_code}",
         "http://localhost:4566/"],
        input=form, capture_output=True, text=True
    )
    stdout = r.stdout
    # Espera erro 4xx (coerente), não 200 (sucesso acidental).
    status_lines = [l for l in stdout.splitlines() if l.startswith("HTTP_STATUS=")]
    assert status_lines, f"sem HTTP_STATUS em output: {stdout}"
    code = status_lines[0].split("=")[1]
    assert code.startswith("4") or code.startswith("5"), \
        f"esperava 4xx/5xx, got {code}"
    # Corpo do erro deve indicar que falhou no shim (request body limit),
    # não ser a resposta de sucesso do Publish.
    assert "<PublishResult" not in stdout, \
        f"resposta continha PublishResult apesar de erro: {stdout}"

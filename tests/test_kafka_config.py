from ssl import SSLContext

import pytest
from pydantic import SecretStr, ValidationError

from dunning.config import Settings
from dunning.kafka import connection_options


def test_plaintext_by_default_for_local_development() -> None:
    assert connection_options(Settings()) == {"security_protocol": "PLAINTEXT"}


def test_sasl_over_tls_for_managed_kafka() -> None:
    options = connection_options(
        Settings(
            kafka_security_protocol="SASL_SSL",
            kafka_sasl_mechanism="SCRAM-SHA-512",
            kafka_sasl_username="dunning",
            kafka_sasl_password=SecretStr("s3cret"),
        )
    )

    assert isinstance(options["ssl_context"], SSLContext)
    assert options["sasl_mechanism"] == "SCRAM-SHA-512"
    assert options["sasl_plain_password"] == "s3cret"


def test_sasl_without_credentials_is_a_configuration_error() -> None:
    with pytest.raises(ValidationError, match="SASL needs"):
        Settings(kafka_security_protocol="SASL_SSL")

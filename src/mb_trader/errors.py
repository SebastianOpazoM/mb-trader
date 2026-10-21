class TraderError(Exception):
    """Base error with a stable machine-readable code."""

    code = "trader_error"

    def as_dict(self) -> dict[str, object]:
        return {"error": self.code, "message": str(self)}


class ConfigurationError(TraderError):
    code = "configuration_error"


class ValidationError(TraderError):
    code = "validation_error"


class GuardError(TraderError):
    code = "guard_error"


class BrokerError(TraderError):
    code = "broker_error"


class BrokerTimeout(BrokerError):
    code = "broker_timeout"


class AmbiguousContract(ValidationError):
    code = "ambiguous_contract"


class IdempotencyConflict(ValidationError):
    code = "idempotency_conflict"


class UncertainOperation(BrokerError):
    code = "uncertain_operation"

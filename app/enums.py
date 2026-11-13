from enum import Enum


class Direction(str, Enum):
    DEPOSIT = "deposit"
    WITHDRAW = "withdraw"


class CardType(str, Enum):
    VISA = "visa"
    MASTERCARD = "mastercard"
    OTHER = "other"


class PaymentStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CONFLICT = "conflict"
    REVIEW_REQUIRED = "review_required"


class AttemptStatus(str, Enum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    DECLINED_FINAL = "declined_final"
    DECLINED_RETRYABLE = "declined_retryable"
    AMBIGUOUS = "ambiguous"
    TIMED_OUT = "timed_out"

from dataclasses import dataclass

from backend.app.classification.models import BOARD_DERIVATION_VERSION


@dataclass(frozen=True)
class SecurityIdentity:
    exchange: str
    board: str
    security_type: str
    derivation_version: str = BOARD_DERIVATION_VERSION


def derive_security_identity(symbol: str, source_type: str) -> SecurityIdentity:
    """Derive one versioned canonical identity from a BaoStock-qualified symbol."""
    exchange = symbol.split(".", 1)[0].lower()
    if source_type == "2":
        return SecurityIdentity(exchange=exchange, board="index", security_type="index")
    if source_type != "1":
        return SecurityIdentity(exchange=exchange, board="other", security_type="other")
    code = symbol.split(".", 1)[1] if "." in symbol else symbol
    if exchange == "bj":
        board = "bse"
    elif exchange == "sh" and code.startswith(("688", "689")):
        board = "star"
    elif exchange == "sz" and code.startswith(("300", "301")):
        board = "chinext"
    elif exchange == "sh" and code.startswith("900"):
        board = "b_share"
    elif exchange == "sz" and code.startswith("200"):
        board = "b_share"
    elif exchange == "sh" and code.startswith(("600", "601", "603", "605")):
        board = "main"
    elif exchange == "sz" and code.startswith(("000", "001", "002", "003")):
        board = "main"
    else:
        board = "other"
    return SecurityIdentity(exchange=exchange, board=board, security_type="stock")

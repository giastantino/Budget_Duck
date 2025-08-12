#!/usr/bin/env python3
"""
Utilities for creating an authenticated Splitwise client.
"""

import keyring
import logging
from splitwise import Splitwise
from typing import Tuple

LOGGER = logging.getLogger(__name__)


def load_splitwise_credentials(user: str) -> Tuple[str, str, str]:
    """
    Fetch Splitwise credentials from the OS keyring.
    Raises EnvironmentError if any secret is missing.
    """
    creds = {}
    for name in ("Consumer_Key", "Consumer_Secret", "API_Key"):
        secret = keyring.get_password(f"Splitwise_{user}_{name}", name)
        if not secret:
            raise EnvironmentError(f"{name} not found in keyring for Splitwise_{user}")
        creds[name] = secret
    return creds["Consumer_Key"], creds["Consumer_Secret"], creds["API_Key"]


def get_splitwise_client(user: str) -> Splitwise:
    """
    Load credentials for `user` and return an authenticated Splitwise client.
    """
    ck, cs, ak = load_splitwise_credentials(user)
    client = Splitwise(ck, cs, api_key=ak)
    LOGGER.info("Authenticated Splitwise client for user=%s", user)
    return client

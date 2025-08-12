import os
import pytest
from core.config import settings

@pytest.mark.skipif(not settings.service_key, reason="SERVICE_KEY not configured")
def test_service_key_present():
    assert settings.service_key
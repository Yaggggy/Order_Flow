import json

from services.broadcaster.main import PUBLISH_LUA, RELEASE_LUA, RENEW_LUA


def test_publish_script_preserves_empty_arrays_and_decimal_strings():
    body = json.dumps({"type": "footprint_snapshot", "candles": [], "delta": "0.00000001"}, separators=(",", ":"))
    encoded = body[:-1] + ',"sequence":42}'
    assert json.loads(encoded) == {"type": "footprint_snapshot", "candles": [], "delta": "0.00000001", "sequence": 42}
    assert "cjson.decode" not in PUBLISH_LUA
    assert "redis.call('GET', KEYS[1]) ~= ARGV[1]" in PUBLISH_LUA
    assert "redis.call('SET', KEYS[3]" in PUBLISH_LUA
    assert "redis.call('PUBLISH', KEYS[4]" in PUBLISH_LUA


def test_lease_renewal_and_release_are_owner_checked():
    assert "redis.call('GET', KEYS[1]) == ARGV[1]" in RENEW_LUA
    assert "redis.call('GET', KEYS[1]) == ARGV[1]" in RELEASE_LUA

import hashlib
import json
from pathlib import Path

root = Path("/opt/kinda")
actual = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((root / "ivy/jars").glob("*.jar"))}
expected = json.loads((root / "maven.lock.json").read_text())
assert actual == expected, {"actual": actual, "expected": expected}
print(json.dumps({"verified_maven_jars": len(actual)}))

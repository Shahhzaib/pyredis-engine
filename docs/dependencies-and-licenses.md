# Dependencies and licenses

PyRedis Engine's own code is provided under the [MIT license](../LICENSE). Runtime
Python code uses the standard library and has no third-party Python dependencies.
Python and the Debian-based container image have their own upstream license notices.

Development and test dependencies include pytest (MIT), pytest-asyncio (Apache-2.0),
Hypothesis (MPL-2.0), redis-py (MIT), Ruff (MIT) and Mypy (MIT). Setuptools and
wheel are used for packaging; the container build pins and verifies their wheel
hashes. Consult each installed distribution's metadata and upstream license for
the exact version and any transitive dependency notices. redis-py is optional and
is not bundled as a runtime server dependency.

Official Redis is used as an external reference during differential tests; it is not
embedded in or distributed as part of the PyRedis runtime image. Review the license
and notices of the particular Redis image/version separately when using it. The
Java architectural reference, whose license was not established, was not included; see
[reference and attribution](reference-and-license.md). The final independent source
and provenance review is still pending.

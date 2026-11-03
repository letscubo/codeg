"""Codeg's one-launch Hermes bootstrap; never edits the installed runtime."""
import importlib.machinery
import importlib.util
import os
import pathlib
import sys


def _chain_original():
    own = pathlib.Path(__file__).resolve().parent
    seen = getattr(sys, "_codeg_hermes_site_seen", None)
    if seen is None:
        seen = sys._codeg_hermes_site_seen = set()
    if str(own) in seen:
        return
    seen.add(str(own))
    search = [p for p in sys.path if str(pathlib.Path(p or os.getcwd()).resolve()) not in seen]
    spec = importlib.machinery.PathFinder.find_spec("sitecustomize", search)
    if spec is not None and spec.loader is not None:
        original = importlib.util.module_from_spec(spec)
        sys.modules["sitecustomize"] = original
        spec.loader.exec_module(original)


# Consume before chaining: tools and provider subprocesses must not install this.
_enabled = os.environ.pop("CODEG_HERMES_NATIVE_BOOTSTRAP", None) == "1"
_chain_original()
if _enabled and ("acp" in sys.argv[1:] or "hermes-acp" in pathlib.Path(sys.argv[0]).name
                 or "acp_adapter.entry" in sys.argv):
    try:
        from codeg_native_steering import install
        install()
    except Exception:
        # Normal Hermes ACP remains available, with no advertised v1 capability.
        pass

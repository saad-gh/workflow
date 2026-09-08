"""
Generic Jinja2 helper functions for workflow_orchestrator.

These are reusable, project-agnostic helpers (XML/SOAP parsing and string/dict
utilities). Project-specific helpers (e.g. integration conditions and body
builders) are NOT defined here; they live in each consumer project and are
loaded via the ``TASK_JINJA_HELPERS`` setting (see ``get_jinja_env``).
"""
import re
import xml.etree.ElementTree as ET

from jinja2 import Environment


def extract_xml(result):
    """Return the raw XML text from a task result dict (or a raw string)."""
    if result is None:
        return ''
    if isinstance(result, str):
        return result
    rj = result.get('response_json') if isinstance(result, dict) else None
    if isinstance(rj, dict) and isinstance(rj.get('text'), str):
        return rj['text']
    body = result.get('body') if isinstance(result, dict) else ''
    return body or ''


def _parse(xml_text):
    try:
        return ET.fromstring(xml_text)
    except Exception:
        return None


def _find_text(root, tag):
    if root is None:
        return None
    tag_low = (tag or '').lower()
    for el in root.iter():
        el_tag = el.tag
        local = el_tag.split('}')[-1].lower() if '}' in el_tag else el_tag.lower()
        if local == tag_low and el.text is not None:
            return el.text
    return None


def xpath_text(result_or_xml, tag):
    return _find_text(_parse(extract_xml(result_or_xml)), tag)


def success_value(result_or_xml):
    return _find_text(_parse(extract_xml(result_or_xml)), 'Success')


def order_status(result_or_xml):
    return _find_text(_parse(extract_xml(result_or_xml)), 'orderStatus')


def remove_symbols(text):
    if text is None:
        return ''
    return re.sub(r'[^a-zA-Z0-9 ]', '', text)


def truncate(text, length=50):
    if not text:
        return ''
    return text[:length - 3] + '...' if len(text) > length else text


def lstrip_prefix_zeros(text):
    if not text:
        return ''
    if text.startswith('SO-'):
        text = text[3:]
    return text.lstrip('0')


def as_dict(result):
    """Return the response_json dict of a task result (or {})."""
    if isinstance(result, dict):
        return result.get('response_json') or {}
    return result or {}


# Generic globals exposed to Jinja2 by default.
JINJA_GLOBALS = {
    'extract_xml': extract_xml,
    'xpath_text': xpath_text,
    'success_value': success_value,
    'order_status': order_status,
    'remove_symbols': remove_symbols,
    'truncate': truncate,
    'lstrip_prefix_zeros': lstrip_prefix_zeros,
    'as_dict': as_dict,
}


def get_jinja_env(**kwargs):
    """
    Return a Jinja2 Environment with the generic helper globals registered,
    plus any project-specific helpers declared via ``TASK_JINJA_HELPERS``.

    ``TASK_JINJA_HELPERS`` is a list of dotted module paths. Each module may
    export a ``JINJA_GLOBALS`` dict which is merged into the environment.
    """
    import importlib

    env = Environment(**kwargs)
    env.globals.update(JINJA_GLOBALS)

    try:
        from django.conf import settings
        helper_modules = getattr(settings, 'TASK_JINJA_HELPERS', []) or []
    except Exception:
        helper_modules = []

    for mod_path in helper_modules:
        try:
            mod = importlib.import_module(mod_path)
        except ImportError:
            continue
        env.globals.update(getattr(mod, 'JINJA_GLOBALS', {}))

    try:
        from django.conf import settings
        loader_path = getattr(settings, 'TASK_JINJA_HELPERS_LOADER', None)
        if loader_path:
            mod_path, fn_name = loader_path.rsplit('.', 1)
            fn = getattr(importlib.import_module(mod_path), fn_name)
            custom_globals = fn()
            if isinstance(custom_globals, dict):
                env.globals.update(custom_globals)
    except Exception:
        pass

    return env

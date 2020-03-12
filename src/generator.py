import shutil
from pathlib import Path
import importlib.util
from functools import partial
from sphinx.cmd import build as sphinx_build
from jinja2 import Template
from textwrap import dedent


analyses = Path(__file__).parent / "analyses"


def import_file(filepath):
    spec = importlib.util.spec_from_file_location(
        Path(filepath).stem, str(filepath)
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def plot(filepath, fn, *args, **kwargs):
    formatted_args = ", ".join(repr(arg) for arg in args)
    return dedent(f"""
    .. pygal::

        import generator
        plot = generator.import_file("{filepath.absolute()}").__dict__["{fn}"]({formatted_args})
    """)


def iter_analyses():
    for analysis in analyses.iterdir():
        if analysis.suffix == ".py" and not analysis.name.startswith("_"):
            yield analysis


if __name__ == "__main__":
    for directory in ["rst", "html"]:
        if Path(directory).exists():
            shutil.rmtree(directory)
        Path(directory).mkdir()

    index = []
    for analysis in iter_analyses():
        module = import_file(analysis)

        template = Template(module.template)
        rendered = template.render(this=module, plot=partial(plot, analysis))

        Path("rst", f"{analysis.stem}.rst").write_text(rendered)
        index.append(analysis.stem)

    Path("rst", "index.rst").write_text(
        ".. toctree::\n"
        + "\n".join(f"    {i}" for i in index)
    )
    sphinx_build.main(["-b", "html", "-c", "src", "rst", "html"])

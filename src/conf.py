import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

# -- Project configuration -----------------------------------------------------
project = "BuMi"

# -- Sphinx configuration -----------------------------------------------------
extensions = [
    'ext.pygal_sphinx_directives',
]

# -- Options for HTML output ---------------------------------------------------
html_theme = "alabaster"
html_description = "BundestagMinig"
html_theme_options = {
    "page_width": "90%",
    "fixed_sidebar": True,
    "caption_font_size": 8,
}

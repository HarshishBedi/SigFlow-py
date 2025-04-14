from setuptools import setup, Extension
from Cython.Build import cythonize

extensions = [
    Extension(
        "engine.parser_cython",
        ["engine/parser_cython.pyx"],
        extra_compile_args=["-O3"],
    )
]

setup(
    name="SigFlowParserCython",
    ext_modules=cythonize(extensions, language_level=3),
)
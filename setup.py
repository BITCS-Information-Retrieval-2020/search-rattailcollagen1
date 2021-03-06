import setuptools

setuptools.setup(
    name="ScienceSearcher",
    version="1.0.6",
    author="RatTailCollagen1",
    author_email="815398117@qq.com",
    description="an IR system for scientific research papers",
    url="https://github.com/BITCS-Information-Retrieval-2020/search-rattailcollagen1",
    packages=setuptools.find_packages(),
    classifiers=["Programming Language :: Python :: 3",
                 "License :: OSI Approved :: MIT License",
                 "Operating System :: OS Independent",
                 ],
    include_package_data=True,
    package_data={'': ['grobid_client/*.json']},
)

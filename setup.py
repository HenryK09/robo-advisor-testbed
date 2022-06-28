from setuptools import setup, find_packages

with open('requirements.txt', encoding='utf-8') as f:
    requirements = []
    for x in f.read().splitlines():
        requirements.append(x)

setup(
    name='robo-advisor-testbed',
    version='0.0.1',
    license='dataknows',
    packages=find_packages(exclude=[]),
    author='bhkang',
    author_email='bohyun.kang@dataknows.ai',
    description='',
    include_package_data=True,
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "robo-advisor-testbed = ratestbed.run.run_process:main",
        ]
    }
)

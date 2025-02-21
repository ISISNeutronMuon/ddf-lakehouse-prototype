=========================
Accelerator Data Platform
=========================

.. include:: common_links.rst

.. toctree::
   :glob:
   :hidden:
   :maxdepth: 2
   :caption: Contents:

   superset/index


Welcome to the landing page for the accelerator data platform. This project aims
at providing a set of tools for collecting, analyzing & visualizing
operational data.

.. warning::

   This platform is currently in active development & testing.
   Please report any issues to `Martyn Gigg <mailto:martyn.gigg@stfc.ac.uk>`_.

Quick links
-----------

- `ISIS Superset <isis-superset_>`_

Getting started
---------------

The platform consists of several components, each focused on a specific purpose.

.. csv-table::
   :header: "Name", "Used for","Prior knowledge assumed"

   :ref:`superset`, Data visualisation or data reporting by building charts and dashboards.,""
   `JupyterHub <jupyterhub_>`_, Online programming environment for running `Jupyter notebooks <project-jupyter_>`_.,"Python, experience with Jupyter notebooks"
   ``Spark``,Large-scale processing on a remote cluster,"`SQL <sql-tutorial_>`_, Python/DataFrames"
   ``Data catalog``, Collecting sets of data from various sources across operations and storing in a common format., "`SQL <sql-tutorial>`_, Python/DataFrames"

If you are unsure where to start then :ref:`superset` is a good place to begin exploring.

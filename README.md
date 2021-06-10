To run project:

1. ``conda create -n aeso_scraper_env python=3.6 numpy pandas requests``
2. ``conda activate aeso_scraper_env``
3. ``python main.py``


Files:
- main.py: entry point
- scraper.py: makes requests to the AESO website, downloads the CSVs, and parses them
- transformer.py: takes parsed response and recreates the AESO table exactly. Then transforms it and returns desired columns
- config.py: contains column definitions, sites we want to filter, peak hours, and directory that reports are saved to
- config_scraper: contains end point configurations as well as whether to print scraping logs
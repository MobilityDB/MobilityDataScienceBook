## Mobility Data Science: From Data to Insights
### Mahmoud Sakr, Alejandro Vaisman, Esteban Zimányi

### Chapter 4: Mobility Visualization

This is the companion website of the book published by Springer.
It contains the datasets used in the book alongside with the scripts
allowing you to input these datasets in PostgreSQL and reproduce the
examples and exercises in the book.

### How to reproduce the Figures
#### Data:
- For Figures 4.4 - 4.10, 4.12, 4.16 & 4.17, the data is stored in `ScheldeData.zip`, and was obtained from http://waterinfo.be, contanining electric conductivity (EC) and temperature (Temp) for some stations (also included in the dataset) between April 1st and 9th, 2022.
- For Figures 4.18 - 4.28, related to the Traffic Analysis in Singapore & Jakarta, the dataset is available upon request sent to grab-posisi.geo@grab.com[^1]. Processing & data ingestion on this dataset occurs in the following files:
    - singapur.sql
    - singapur_input.sql
    - jakarta.sql
    - jakarta_input.sql
- For Figures 4.29 - 4.31, related to the Air Quality Analysis in Delhi, the data can be downloaded from Processing & data ingestion on this dataset occurs in the following files:
    - delhiscript.sql
    - delhi_input.sql

[^1]: See https://engineering.grab.com/grab-posisi.


#### Code:
- For Figures 4.4 - 4.8, see `timehistogramSalinity.ipynb`.
- For Figures 4.10 & 4.12, see `2dtimeshartsalinity-andTimeHistogram.ipynb`.
- For Figure 4.11, see `ganttchart.ipynb`.
- For the Traffic Analysis in Singapore, see `singaporeclusteringandheatmap.ipynb`.
- For the Traffic Analysis in Jakarta, see `Jakarta-heatmap.ipynb`

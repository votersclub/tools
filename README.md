# Votersclub

The project is dedicated to provide the data about the results of the elections in Russian Federation in a
format convenient to apply methods of machine learning to investigate results of the elections.

Web site http://votersclub.org uses the data set created by this toolset. It's all in Russian because source data are
heavily Russian based (names, locations, etc).

You can download an archive with the results of elections at http://votersclub.org/about

## Getting Started

The toolset in this repository consists of the several scripts which download and parse the results of the elections published by elections committee of Russian Federation.
Results are saved in .csv (data) and .json (metadata) formats.

You can use ipython or any other means you prefer to process the data.

To get the toolset use command

git clone https://github.com/votersclub/tools.git

### Prerequisites

You need to install BeatifulSoap, pandas, numpy and may be several other python libraries used by the toolset.

To do that you can use commands
pip install beautifulsoup4
pip install pandas
pip install numpy
...


## Running the tests

Tests are still to be written. However, you can check the results of the data processing using provided script
generate_passport.py which creates sort of digital passport of the data which are there in the results directory. It shows you
 number of the data files, metadata files, candidates, votes, commissions, empty files, data errors, etc.

You can redirect output of the script to the textfile and use this textfile to compare results of different executions, like this:

python ./generate_passport.py > profile.txt 2>&1


## Usage

1. Create the directory named 'data' in the directory with scripts
2. Open http://www.vybory.izbirkom.ru/region/izbirkom in your browser
3. On top left part of the page select the date interval you're interested in
4. On top right part of the page select the level of the elections you're interested in. As of now only
one of the 'Федеральный' (Federal) or 'Региональный' (Regional) is supported (only one of them at a time).
Other levels of elections are not yet supported.
5. Press 'Искать' (Search)
6. Save the resulting web page with the name 'region_list.htm' (for regional elections) or 'federal_list.htm' (for federal)
in the data directory you created at step 1
7. Run the script loader.py with parameter 'subject' or 'federal' like this:

python loader.py federal >> federal.log 2>&1
or
python loader.py subject >> subject.log 2>&1

Script processes the list of the elections from htm file, creates intermediate config file subject_elections_list.jsn or
federal_elections_list.jsn (depending on parameter) and one by one downloads and parses results of the elections from this list.

All the data files, metadata files and intermediate files are stored in the data directory created at step 1.

## Authors

Gosha votersclub@protonmail.org


## License

This project is licensed under the Apache 2 License - see the [LICENSE.md](LICENSE.md) file for details

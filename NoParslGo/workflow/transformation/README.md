Rescale, standardize, normalize, binarize : Apply these to int, float columns only

Fix python scripts and user script for follwoig inputs
1. Rescale : input = lowerBoundry, upperBoundry
2. Standardize : no input
3. Normalize : no input
4. Binarize : input = userThreshold
threshold : Values less than or equal to threshold is mapped to 0, else to 1. By default threshold value is 0.0. (from definition. no need of having default value. Ask user for threshold)

The four methods above are to be applied for user selected columns. So input should include an array of columns.

For the static workflow we would apply transformation in the following sequence;
(specific to our GDELT input)
1. Standardize : AvgTone (since there are outliers. "Score between -100 to +100, with common values between -10 to +10")
2. Rescale : NumMentions, NumSources, NumArticles.
lowerBoundry = 0, upperBoundry = 100
3. Binarization : NumMentions, NumSources, NumArticles.
Lets binarize these articles at a suitable threshold- maybe 10? above which the user could consider an article as significant.
The user should be able to define seperate thresholds for each column (ordered dict)

There aren't enough numeric columns to do all these operations on the GDELT dataset



Next lets do binning/categorize/encoding.

User input parameters are as follows;
1. Categorize : Input = number of categories
2. Binning : Input = number of bins, bin labels
3. Encoding : no input

The four methods above are to be applied for user selected columns. So input should include an array of columns.

LOOK INTO : How do we handle things when we have 8 data items and 3 categories/bins? is it divided as 3/3/2 automatically? (@AMANDA do you know this?)

For the static workflow on the GDELT data set;
I don't see a column specifically requiring discretization, it seems to have been already done on the dataset.
The only discretizaation I can think of is for the EventBaseCode column.
eg : 025 = appeal to yield
    0251 = appeal for easing og administrative sanctions
    we can categorize 025 and 0251 in the 'appeal category'
    This is a stretch. I dont think there are suitable columns for this on our dataset.

Additional Note for future development work:

A distribution could be any of the below. We would later on make this workflow dynamic by first drawing the distribution and then determining the best transformation mechanism to apply (in order to make it a normal distribution, eliminate/reduce effect of outliers etc.)

beta — with negative skew
exponential — with positive skew
normal_p — normal, platykurtic
normal_l — normal, leptokurtic
bimodal — bimodal

from mrjob.job import MRJob
import math

# Define the number of bins for the histogram
NUM_BINS = 10

class SummaryStats (MRJob):

    def mapper (self, _, line):
        # Split the line by tab and get the value
        _, _, value = line.split ('\t')
        value = float (value)
        # Emit a tuple of (None, value)
        yield None, value

    def combiner (self, _, values):
        # Initialize the variables for the summary statistics
        count = 0
        sum = 0
        sum_sq = 0
        min = float ('inf')
        max = float ('-inf')
        # Initialize the list for the histogram counts
        hist = [0] * NUM_BINS
        # Initialize the list for the values
        vals = []
        # Loop through the values
        for value in values:
            # Update the count, sum, and sum of squares
            count += 1
            sum += value
            sum_sq += value ** 2
            # Update the min and max
            if value < min:
                min = value
            if value > max:
                max = value
            # Update the histogram counts
            # Assume the range of values is [0, 10]
            bin_index = int (value * NUM_BINS / 10)
            if bin_index == NUM_BINS:
                bin_index -= 1
            hist [bin_index] += 1
            # Append the value to the vals list
            vals.append (value)
        # Compute the mean and standard deviation
        mean = sum / count
        std = math.sqrt (sum_sq / count - mean ** 2)
        # Create a list of the summary statistics and histogram counts
        stats = [count, mean, std, min, max] + hist
        # Emit a tuple of (None, stats, vals)
        yield None, (stats, vals)

    def reducer (self, _, stats_vals_list):
        # Initialize the variables for the summary statistics
        total_count = 0
        total_sum = 0
        total_sum_sq = 0
        total_min = float ('inf')
        total_max = float ('-inf')
        # Initialize the list for the histogram counts
        total_hist = [0] * NUM_BINS
        # Initialize the list for the values
        total_vals = []
        # Loop through the stats_vals list
        for stats, vals in stats_vals_list:
            # Unpack the stats list
            count, mean, std, min, max, *hist = stats
            # Update the total count, sum, and sum of squares
            total_count += count
            total_sum += count * mean
            total_sum_sq += count * (std ** 2 + mean ** 2)
            # Update the total min and max
            if min < total_min:
                total_min = min
            if max > total_max:
                total_max = max
            # Update the total histogram counts
            for i in range (NUM_BINS):
                total_hist [i] += hist [i]
            # Extend the total_vals list with the vals list
            total_vals.extend (vals)
        # Compute the final mean and standard deviation
        final_mean = total_sum / total_count
        final_std = math.sqrt (total_sum_sq / total_count - final_mean ** 2)
        # Sort the total_vals list
        total_vals.sort ()
        # Compute the median (or an approximation of the median)
        if total_count % 2 == 0:
            # If the total count is even, take the average of the middle two elements
            median = (total_vals [total_count // 2 - 1] + total_vals [total_count // 2]) / 2
        else:
            # If the total count is odd, take the middle element
            median = total_vals [total_count // 2]
        # Create a list of the final summary statistics and histogram counts
        final_stats = [total_count, final_mean, final_std, total_min, total_max, median] + total_hist
        # Emit a tuple of (None, final_stats)
        yield None, final_stats

        
if __name__ == '__main__':
    SummaryStats.run ()

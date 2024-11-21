from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np

class KMeansStep(MRJob):

    def configure_args(self):
        super(KMeansStep, self).configure_args()
        # Add arguments to get the path to the centroids file
        self.add_passthru_arg('--centroids', help='Path to centroids file')

    def load_centroids(self):
        # Load centroids from the file
        centroids = {}
        with open(self.options.centroids, 'r') as f:
            for line in f:
                centroid_id, _, _ = line.split('\t')
                centroids[int(centroid_id)] = np.array([float(x) for x in line.split('\t')[1:]])
        return centroids

    def assign_mapper(self, _, line):
        # Mapper: Assign each data point to the closest centroid
        data_point = np.array([float(x) for x in line.split('\t')])
        centroids = self.load_centroids()

        # Find the closest centroid
        closest_centroid = min(centroids.keys(), key=lambda c: np.linalg.norm(data_point[1:] - centroids[c]))

        yield closest_centroid, data_point

    def assign_reducer(self, centroid, data_points):
        # Reducer: Recompute the centroid based on assigned data points
        new_centroid = np.mean(list(data_points), axis=0)

        yield centroid, new_centroid

    def steps(self):
        return [
            MRStep(mapper=self.assign_mapper, reducer=self.assign_reducer)
        ]

if __name__ == '__main__':
    KMeansStep.run()

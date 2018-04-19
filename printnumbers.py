'''
  ************
  python setup
  *************
 1. Download and install miniconda
    https://conda.io/docs/user-guide/install/macos.html
 1. show existing python virtual env
    conda info --env
 2. create python virtual env
    conda create --name  sample_luigi  python=3.6
 3. Activate python virtual env
    source activate sample_luigi
 4. Install libraries
    pip install --index-url=https://artifacts.merck.com/artifactory/api/pypi/pypi-main/simple -r requirements.txt
 
'''

'''
  ************
  App setup
  *************
  a. Run without web UI
    python printnumbers.py  SquaredNumbers --local-scheduler --n 20
  
  b. View progress on web UI
    1.In a separate terminal / SSH window, start up the daemon
        luigid
    2.Then, in a browser, fire up the following web address
        http://localhost:8082
    3: Run luigi on deamon
        session 1: python printnumbers.py --scheduler-host localhost SquaredNumbers --n 200000
        session 2: python printnumbers.py --scheduler-host localhost SquaredNumbers --n 200000
        session 3: python printnumbers.py --scheduler-host localhost SquaredNumbers --n 200000
'''


import luigi

class PrintNumbers(luigi.Task):
    n = luigi.IntParameter(default=10)

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget("numbers_up_to_{}.txt".format(self.n))

    def run(self):
        with self.output().open('w') as f:
            for i in range(1, self.n+1):
                f.write("{}\n".format(i))

class SquaredNumbers(luigi.Task):
    n = luigi.IntParameter(default=10)

    def requires(self):
        return [PrintNumbers(n=self.n)]

    def output(self):
        return luigi.LocalTarget("squares_up_to_{}.txt".format(self.n))

    def run(self):
        with self.input()[0].open() as fin, self.output().open('w') as fout:
            for line in fin:
                n = int(line.strip())
                out = n * n
                fout.write("{}:{}\n".format(n, out))

if __name__ == '__main__':
    luigi.run()
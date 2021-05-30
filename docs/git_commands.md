## Count number of commits between branches

Count number of commits between `main` and `origin/main`
```
trantor $ git lg
* a418cbb (HEAD -> main, lessons/spark_tests) added TimeOfMostRequests
* 5c97fb5 add log4j config
* 1d31354 add MostPopular
* 143b891 add sbt settings from previous homework
* ad26b3d refactor initial commit
* 3b5c95b update main Readme
* 9b1b5a1 started hw spark unit test
* fadfd1f (origin/main, origin/HEAD) rename dirs
* deaa8a2 Update readme
...
```

### using git log
```
git log --oneline main ^origin/main | wc -l
7
```

### using git rev-list
```
git rev-list --count origin/main..main
```

## Move changes to another branch

### Create new branch out of current
```
git branch lessons/spark_tests
```

### Rewind commits on the current branch
```
git reset --hard HEAD~7
```

### Switch to new branch
```
git switch  lessons/spark_tests
```

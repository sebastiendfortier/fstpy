# Use as git submodule
## Idea of git submodules

A commit means a fixed state of our repo.  I.E. We checkout a commit, everything
is as it was when we made that commit.

With git submodules, this is simply achieved by saying that a commit of the
super repo is

- Fixed states for all the files
- Fixed commits for all the submodule repos.

Checking out a commit of the super repo does two things to bring back exactly
the state of that commit.

- Brings back all the files from that commit
- For all the submodules checkout the right commits for them.

If we modify files in the submodule or if we checkout a different commit in the
submodule, this shows a a change for the super repo.

The change shows as "Submodule X : modified content".

This means that in effect *a commit of the super repo still reliably and simply
represents a fixed state of the repo and its submodules*

## Add this repo as a submodule

```bash
git submodule add https://gitlab.science.gc.ca/sbf000/fstpy
git commit -m "Added fstpy as submodule of this repo"
```

The diff:
```diff
commit 26ba3eca8b07ab3808f41fa486499706d67c1615 (HEAD -> master)
Author: Philippe Carphin <philippe.carphin2@canada.ca>
Date:   Sat Jan 30 10:54:02 2021 -0500

    Added fstpy as submodule of this repo

diff --git a/.gitmodules b/.gitmodules
new file mode 100644
index 0000000..d52740a
--- /dev/null
+++ b/.gitmodules
@@ -0,0 +1,3 @@
+[submodule "fstpy"]
+       path = fstpy
+       url = https://gitlab.science.gc.ca/sbf000/fstpy
diff --git a/fstpy b/fstpy
new file mode 160000
index 0000000..f94438d
--- /dev/null
+++ b/fstpy
@@ -0,0 +1 @@
+Subproject commit f94438d908aaf7de0078738a0777cd390027e46e
```
## Cloning when using as a submodule

## Switching to a different commit in the submodule
If you decide to use a different version of fstpy for your program, then `cd`
into the submodule, fetch, checkout the version that you want.

Then in the super repo, make a commit saying 


```bash
[super-repo] $ cd fstpy
[fstpy] $ git fetch
...
[fstpy] $ git checkout origin/dev
...
[fstpy] $ cd ..
```
The change looks like this from the point of view of the super-repo:
```bash
[Super repo] $ git status
On branch master
Changes not staged for commit:
(use "git add <file>..." to update what will be committed)
(use "git checkout -- <file>..." to discard changes in working directory)

      modified:   fstpy (new commits)

```
We add `fstpy` as any file and make our commit:
```bash
[super-repo] $ git add fstpy
[super-repo] $ git status
On branch master
Changes to be committed:
(use "git reset HEAD <file>..." to unstage)

      modified:   fstpy

[super-repo] $ git commit -m "Switched to different version of fstpy"
```
What is the diff of this commit?
```diff
t 37d0be16d7d766b4561a573655ef95073643f473 (HEAD -> master)
Author: Philippe Carphin <philippe.carphin2@canada.ca>
Date:   Sat Jan 30 11:04:30 2021 -0500

    Switched to a different version of fstpy

diff --git a/fstpy b/fstpy
index f94438d..78e3ea4 160000
--- a/fstpy
+++ b/fstpy
@@ -1 +1 @@
-Subproject commit f94438d908aaf7de0078738a0777cd390027e46e
+Subproject commit 78e3ea4a48b756f4f61565aa098dd5dc3d6a0200
```

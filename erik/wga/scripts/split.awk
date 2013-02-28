#http://superuser.com/questions/485189/using-awk-to-split-text-file-every-10-000-lines

BEGIN { file = "1" }

{ print | "gzip -9 > " file ".gz" }

NR % 10000 == 0 {
  close("gzip -9 > " file ".gz")
  file = file + 1
}
import os
import argparse
import hashlib
import boto3        # type: ignore
from concurrent.futures import ThreadPoolExecutor
from typing import Set, Dict
from botocore.exceptions import NoCredentialsError      # type: ignore


class S3SyncTool:
    def __init__(self, localFolder: str, bucketName: str, prefix: str, dryRun: bool, maxThreads: int = 5) -> None:
        self.localFolder = localFolder
        self.bucketName = bucketName
        self.prefix = prefix
        self.dryRun = dryRun
        self.s3 = boto3.client('s3')
        self.maxThreads = maxThreads

    def listLocalFiles(self) -> Dict[str, str]:
        """Listet lokale Dateien mit ihren MD5-Hashes."""
        files = {}
        for root, _, filenames in os.walk(self.localFolder):
            for filename in filenames:
                fullPath = os.path.join(root, filename)
                relativePath = os.path.relpath(fullPath, self.localFolder).replace("\\", "/")
                files[relativePath] = self.calculateMd5(fullPath)
        return files

    def listS3Objects(self) -> Dict[str, str]:
        """Listet S3 Objekte und deren ETag (meist MD5 Hash)."""
        paginator = self.s3.get_paginator('list_objects_v2')
        pageIterator = paginator.paginate(Bucket=self.bucketName, Prefix=self.prefix)

        objects = {}
        for page in pageIterator:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if self.prefix:
                    relativeKey = os.path.relpath(key, self.prefix).replace("\\", "/")
                else:
                    relativeKey = key
                etag = obj['ETag'].strip('"')
                objects[relativeKey] = etag
        return objects

    def calculateMd5(self, filePath: str) -> str:
        """Berechnet den MD5 Hash einer Datei."""
        hashMd5 = hashlib.md5()
        with open(filePath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hashMd5.update(chunk)
        return hashMd5.hexdigest()

    def uploadFile(self, localFile: str) -> None:
        """Lädt eine Datei in den S3-Bucket hoch."""
        s3Key = os.path.join(self.prefix, localFile).replace("\\", "/") if self.prefix else localFile
        localPath = os.path.join(self.localFolder, localFile)

        if self.dryRun:
            print(f"[DryRun] Würde hochladen: {s3Key}")
            return

        self.s3.upload_file(localPath, self.bucketName, s3Key)
        print(f"Hochgeladen: {s3Key}")

    def deleteS3Object(self, s3Object: str) -> None:
        """Löscht eine Datei im S3-Bucket."""
        s3Key = os.path.join(self.prefix, s3Object).replace("\\", "/") if self.prefix else s3Object

        if self.dryRun:
            print(f"[DryRun] Würde löschen: {s3Key}")
            return

        self.s3.delete_object(Bucket=self.bucketName, Key=s3Key)
        print(f"Gelöscht: {s3Key}")

    def sync(self) -> None:
        """Synchronisiert den lokalen Ordner mit dem S3-Bucket."""
        print("Starte Synchronisation...")

        localFiles = self.listLocalFiles()
        s3Objects = self.listS3Objects()

        uploads = []
        deletes = []

        # Dateien, die hochgeladen oder ersetzt werden müssen
        for localFile, localMd5 in localFiles.items():
            s3Md5 = s3Objects.get(localFile)
            if not s3Md5:
                print(f"Datei fehlt im S3: {localFile}")
                uploads.append(localFile)
            elif localMd5 != s3Md5:
                print(f"Datei geändert: {localFile}")
                uploads.append(localFile)
            else:
                print(f"Datei aktuell: {localFile}")

        # Dateien, die im S3 gelöscht werden sollen
        for s3Object in s3Objects.keys():
            if s3Object not in localFiles:
                print(f"Datei verwaist im S3: {s3Object}")
                deletes.append(s3Object)

        # Uploads durchführen
        if uploads:
            print(f"{len(uploads)} Dateien werden hochgeladen...")
            with ThreadPoolExecutor(max_workers=self.maxThreads) as executor:
                executor.map(self.uploadFile, uploads)

        # Löschungen durchführen
        for s3Object in deletes:
            self.deleteS3Object(s3Object)

        print("Synchronisation abgeschlossen.")


def parseArguments() -> argparse.Namespace:
    """Parst die Kommandozeilenargumente."""
    parser = argparse.ArgumentParser(description="Synchronisiere einen lokalen Ordner mit einem S3 Bucket.")
    parser.add_argument('--localFolder', required=True, help="Pfad zum lokalen Ordner")
    parser.add_argument('--bucketName', required=True, help="Name des S3 Buckets")
    parser.add_argument('--prefix', default='', help="Prefix im S3 Bucket (optional)")
    parser.add_argument('--dryRun', action='store_true', help="Nur anzeigen, was passieren würde")
    parser.add_argument('--threads', type=int, default=5, help="Anzahl gleichzeitiger Upload-Threads (Standard 5)")

    return parser.parse_args()


def main() -> None:
    args = parseArguments()

    try:
        syncTool = S3SyncTool(
            localFolder=args.localFolder,
            bucketName=args.bucketName,
            prefix=args.prefix,
            dryRun=args.dryRun,
            maxThreads=args.threads
        )
        syncTool.sync()
    except NoCredentialsError:
        print("Fehler: Keine AWS Credentials gefunden. Stelle sicher, dass IAM Rollen oder Umgebungsvariablen korrekt gesetzt sind.")


if __name__ == "__main__":
    main()

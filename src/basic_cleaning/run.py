#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact.
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)
    
    logger.info("Downloading and reading the artifact")
    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    # YOUR CODE 
    # -------------------------------------
    df = pd.read_csv(artifact_local_path)

    logger.info("Set price boundaries and clean 'last review' datetype.")

    min_price_b = args.min_price
    max_price_b = args.max_price

    idx = df['price'].between(min_price_b, max_price_b)
    df = df[idx].copy()

    # recast 'last_review' column
    df['last_review'] = pd.to_datetime(df['last_review'])

    # insertion for release 1.0.1
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    logger.info("Upload Aritifact to W&B.")

    df.to_csv("clean_sample.csv", index=False)

    artifact = wandb.Artifact(
            args.output_artifact,
            type=args.output_type,
            description=args.output_description,
            )

    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Fully-qualified name for the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Fully Qualified name for the Output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the Output Artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description for the Output Artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum rental price we want to have in our dataset. Default=5",
        default=5.0,
        required=False
    )

    parser.add_argument(
        "--max_price", 
        type=float, 
        help="Maximum rental price we want to have in our dataset. Default=500",
        default=500.0,
        required=False
    )


    args = parser.parse_args()

    go(args)

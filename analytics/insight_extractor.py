# Databricks notebook source
!pip install pwlf

# COMMAND ----------

class InsightExtractor:
    def __init__(self, X,Y):
        self.X = X
        self.Y = Y
        self.TrendDetector = TrendDetector()

    def get_volatility(self):
        # Returns the CV (%)
        return (self.Y.std() / self.Y.mean()) * 100

    def get_structural_segments(self):
        return self.TrendDetector.extract_trend(self.X, self.Y)

    def extract_full_suite(self):
        # Returns a dictionary ready for your Delta Table
        return {
            "cv_value": self.get_volatility(),
            "segments": self.get_structural_segments(),
        }

# COMMAND ----------

import pwlf
import pandas as pd
import numpy as np

class TrendDetector:
    def __init__(self, max_segments=3, threshhold=0.05):
        self.max_segments = max_segments
        self.threshhold = threshhold

    def fit_best_model(self, x, y):
        """Iterates through segment counts and returns the best-fitting pwlf object."""
        best_model = None

        # Test from 1 segment (straight line) up to max_segments
        for n_seg in range(1, self.max_segments + 1):
            # check for degree of freedom violation
            if len(x) - n_seg <= 1:
                continue
            try:
                model = pwlf.PiecewiseLinFit(x, y)
                model.fit(n_seg)
                for p_val in model.p_values()[1:]:
                    if p_val > self.threshhold:
                        model= None
                if model:
                    best_model = model
                                
                
            except Exception as e:
                print(f"Fit failed for {n_seg} segments: {e}")
                continue
                
        return best_model

    def extract_trend(self, x, y, metadata={}):
        """
        Main entry point: Fits the model and returns a list of 
        significant breakpoints as dictionaries.
        """
        model = self.fit_best_model(x, y)
        if model is None:
            return []
        p_values = model.p_values()
        slopes = np.cumsum(model.beta[1:])
        breaks = model.fit_breaks
        segments = []
        for i in range(0, len(breaks)-1):
            start_year = round(breaks[i])
            end_year = round(breaks[i+1])
            start_index = np.searchsorted(x, start_year)
            end_index = np.searchsorted(x, end_year)   
            segment = {
                'start_value': y[start_index],
                'end_value': y[end_index],
                "start_year": start_year,
                "end_year": end_year,
                'slope': slopes[i],
                "p_value": p_values[i],
            }
            segments.append(segment)
            

        return segments

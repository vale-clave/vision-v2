from datetime import datetime

import numpy as np

import supervision as sv


class ClockBasedTimer:
    """
    A timer that calculates the duration each object has been detected based on the
    system clock.

    Attributes:
        tracker_id2start_time (Dict[int, datetime]): Maps each tracker's ID to the
            datetime when it was first detected.
    """

    def __init__(self) -> None:
        """Initializes the ClockBasedTimer."""
        self.tracker_id2start_time: dict[int, datetime] = {}

    def tick(self, detections: sv.Detections) -> np.ndarray:
        """Processes the current frame, updating time durations for each tracker.

        Args:
            detections: The detections for the current frame, including tracker IDs.

        Returns:
            np.ndarray: Time durations (in seconds) for each detected tracker, since
                their first detection.
        """
        current_time = datetime.now()
        times = []

        for tracker_id in detections.tracker_id:
            self.tracker_id2start_time.setdefault(tracker_id, current_time)

            start_time = self.tracker_id2start_time[tracker_id]
            time_duration = (current_time - start_time).total_seconds()
            times.append(time_duration)

        return np.array(times)


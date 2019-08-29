package com.codingme.eshelper.summary;

import java.util.Objects;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SummaryIndex {

	private int endCharIndex;
	private int endListIndex;
	private int length;
	private double score;
	private int startCharIndex;
	private int startListIndex;

	public SummaryIndex() {
	}

	public SummaryIndex(int start, int end, int startIndex, int endIndex) {
		this.startCharIndex = start;
		this.endCharIndex = end;
		this.startListIndex = startIndex;
		this.endListIndex = endIndex;
		this.length = end - start;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SummaryIndex that = (SummaryIndex) o;
		return startCharIndex == that.startCharIndex && startListIndex == that.startListIndex
				&& endCharIndex == that.endCharIndex && endListIndex == that.endListIndex && length == that.length;
	}

	@Override
	public int hashCode() {
		return Objects.hash(startCharIndex, startListIndex, endCharIndex, endListIndex, length);
	}
}
package com.livingsocial.hive.udf;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

/**
 * Tokenize: splits a natural language chunk of text into an array of stemmed
 * lowercase words. English stop words are excluded from the output.
 * 
 */
@Description(name = "tokenize", value = "_FUNC_(str) - Splits str"
		+ " into an arrays of stemmed words")
public class Tokenize extends UDF {

	public ArrayList<Text> evaluate(Text text) throws HiveException {
		ArrayList<Text> result = new ArrayList<Text>();
		Analyzer analyzer = new MyAnalyzer();
		try {
			TokenStream stream = analyzer.tokenStream("",
					new StringReader(text.toString()));
			stream.reset();
			while (stream.incrementToken()) {
				CharTermAttribute term = stream
						.getAttribute(CharTermAttribute.class);
				result.add(new Text(term.toString()));
			}
		} catch (IOException e) {
			throw new HiveException(e);
		} finally {
			analyzer.close();
		}
		return result;
	}

	private static class DefaultSetHolder {
		static final CharArraySet DEFAULT_STOP_SET = StandardAnalyzer.STOP_WORDS_SET;
	}

	/**
	 * Customer Analyzer based on {@link StandardAnalyzer} except using
	 * {@link KStemFilter} instead of the more aggressive 
	 * {@link PorterStemFilter}. I also added in the {@link ASCIIFoldingFilter}
	 * in order to remove accents from words, and {@link HTMLStripCharFilter}
	 * to strip out HTML elements.
	 */
	private static class MyAnalyzer extends Analyzer {

		@Override
		protected TokenStreamComponents createComponents(String fieldName,
				Reader reader) {
			Version matchVersion = Version.LUCENE_45;
			final Tokenizer source = new StandardTokenizer(matchVersion, reader);
			TokenStream result = new StandardFilter(matchVersion, source);
			result = new EnglishPossessiveFilter(matchVersion, result);
			result = new LowerCaseFilter(matchVersion, result);
			result = new StopFilter(matchVersion, result,
					DefaultSetHolder.DEFAULT_STOP_SET);
			result = new ASCIIFoldingFilter(result);
			result = new KStemFilter(result);
			return new TokenStreamComponents(source, result);
		}

		@Override
		protected Reader initReader(String fieldName, Reader reader) {
			return new HTMLStripCharFilter(reader);
		}
	}
}
/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.icatproject.elasticsearch.exceptions;

/**
 *
 * @author dhiwatdg
 */

@SuppressWarnings("serial")
public class ElasticsearchException extends Exception {

	private int httpStatusCode;
	private String message;

	public  ElasticsearchException(int httpStatusCode, String message) {
		this.httpStatusCode = httpStatusCode;
		this.message = message;
	}

	public String getShortMessage() {
		return message;
	}

	public int getHttpStatusCode() {
		return httpStatusCode;
	}

	public String getMessage() {
		return "(" + httpStatusCode + ") : " + message;
	}

}

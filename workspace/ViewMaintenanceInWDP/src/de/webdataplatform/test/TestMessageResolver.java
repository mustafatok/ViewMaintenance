package de.webdataplatform.test;

import java.util.List;

import de.webdataplatform.viewmanager.MessageResolver;

public class TestMessageResolver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {


		MessageResolver messageResolver = new MessageResolver();
		
		String nachricht0 = "<basetable;rs1;3213709;k08020;Put;aggregationKey=x0911,ag";
		String nachricht1 = "gregationValue=9; ><basetable;rs1;3213710;k08020;DeleteFamily;=;aggregationKey=x0911,aggregationValue=9><basetable;rs1;3213711;k08022;Put;aggregationKey=x0814,aggregationValue=5; ><basetable;rs1;3213712;k08022;Put;aggregationKey=x0225,aggregationValu";
		String nachricht2 = "e=3;aggregationKey=x0814,aggrega";
		String nachricht3 = "tionValue=5>";
		List<String> updates = messageResolver.resolve(nachricht0);
		System.out.println(updates);
		updates = messageResolver.resolve(nachricht1);
		System.out.println(updates);
		updates = messageResolver.resolve(nachricht2);
		System.out.println(updates);
		updates = messageResolver.resolve(nachricht3);
		System.out.println(updates);
	}

}

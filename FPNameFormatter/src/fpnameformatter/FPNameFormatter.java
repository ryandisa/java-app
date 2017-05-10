/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fpnameformatter;

import java.io.File;
import javax.swing.JOptionPane;

/**
 *
 * @author refly.maliangkay
 */
public class FPNameFormatter {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        File currentDir = new File("").getAbsoluteFile();
        for (final File fileEntry : currentDir.listFiles()) {
            if (fileEntry.isDirectory()) {
            } else {
                if (fileEntry.getName().contains("033291071015000") && !fileEntry.getName().contains("-000000000000000-") && fileEntry.getName().endsWith(".pdf")) {
                    String name = fileEntry.getName().substring(fileEntry.getName().indexOf("-", 1) + 1);
                    name = name.substring(0, name.indexOf("-"));
                    fileEntry.renameTo(new File(currentDir.getAbsolutePath() + "\\" + name + ".pdf"));
                }
            }
        }
    }
}

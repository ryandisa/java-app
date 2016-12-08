/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package scheduledexportcsv;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import javax.swing.JFileChooser;

/**
 *
 * @author Refly IDFA
 */
public class TestReadQuery {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        String query = "";

        JFileChooser chooser = new JFileChooser();
        chooser.showDialog(null, "Select Folder");

        String sql = chooser.getSelectedFile().getAbsolutePath();

        try {
            BufferedReader bufferedReader = new BufferedReader(
                    new FileReader(sql)
            );
            int i = 0;
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                sb.append(line + "\n");
            }
            query = sb.toString();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(query);
    }

}
